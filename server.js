require('dotenv').config();
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const cluster = require('cluster');
const { execSync } = require('child_process');
const os = require('os');
const { pipeline } = require('stream');

if (cluster.isMaster) cluster.schedulingPolicy = cluster.SCHED_RR;

const PORT = process.env.PORT || 443;
const HTTP_PORT = process.env.HTTP_PORT || 80;
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || 'public');
const SSL_KEY = process.env.SSL_KEY || 'ssl/privkey.pem';
const SSL_CERT = process.env.SSL_CERT || 'ssl/fullchain.pem';
const DOMAIN = process.env.DOMAIN || 'localhost';
const MAX_FILE_SIZE = parseInt(process.env.MAX_FILE_SIZE) || 0;
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT) || 100;
const RATE_WINDOW = parseInt(process.env.RATE_WINDOW) || 60000;
const ALLOWED_SUBDIRS = (process.env.ALLOWED_SUBDIRS || '')
  .split(',').map(s => s.trim().replace(/^\/+|\/+$/g, '')).filter(Boolean);
const STREAM_HIGH_WATER_MARK = parseInt(process.env.STREAM_HIGH_WATER_MARK) || 1048576;
const NUM_WORKERS = (parseInt(process.env.NUM_WORKERS) || os.cpus().length)-1;
const MEMORY_CACHE_MAX_SIZE = parseInt(process.env.MEMORY_CACHE_MAX_SIZE) || 1073741824;
const REDIS_CACHE_MAX_SIZE = parseInt(process.env.REDIS_CACHE_MAX_SIZE) || 104857600;
const REDIS_URL = process.env.REDIS_URL || '';
const MAX_CACHEABLE_FILE_SIZE = parseInt(process.env.MAX_CACHEABLE_FILE_SIZE) || 26214400; // 25MB
const MAX_CONCURRENT_STREAMS = parseInt(process.env.MAX_CONCURRENT_STREAMS) || 80;

const TOTAL_RAM = os.totalmem();
const RESERVED_RAM = Math.max(Math.floor(TOTAL_RAM * 0.15), 419430400);
const USABLE_RAM = Math.max(TOTAL_RAM - RESERVED_RAM, 0);
const PER_WORKER_RAM = Math.floor((USABLE_RAM * 0.6) / Math.max(NUM_WORKERS, 1));
const PER_WORKER_CACHE = Math.min(MEMORY_CACHE_MAX_SIZE, Math.floor(PER_WORKER_RAM * 0.7));
const WORKER_MAX_OLD_SPACE_MB = Math.max(128, Math.floor(PER_WORKER_RAM / (1024 * 1024)));
const MAX_INFLIGHT_BUFFER_BYTES = Math.max(MAX_CACHEABLE_FILE_SIZE * 2, Math.floor(PER_WORKER_CACHE * 0.4));
let inFlightBufferBytes = 0;
let activeStreams = 0;

const l1Cache = new Map();
const l1Queue = [];
let l1Size = 0;

function l1Set(key, buf) {
  while (l1Size + buf.length > PER_WORKER_CACHE && l1Queue.length > 0) {
    const evictKey = l1Queue.shift();
    const old = l1Cache.get(evictKey);
    if (old) { l1Size -= old.length; l1Cache.delete(evictKey); }
  }
  if (!l1Cache.has(key)) l1Queue.push(key);
  l1Cache.set(key, buf);
  l1Size += buf.length;
}

function l1Get(key) {
  return l1Cache.get(key) || null;
}

const startedAt = Date.now();
let lastMemCheck = 0;

function checkMemory() {
  const now = Date.now();
  if (now - lastMemCheck < 5000) return;
  lastMemCheck = now;
  const usage = process.memoryUsage();
  const heapPercent = Math.round((usage.heapUsed / usage.heapTotal) * 100);
  if (heapPercent > 85 || usage.rss > PER_WORKER_RAM * 0.9) {
    if (l1Size > 0) {
      const targetSize = Math.floor(l1Size * 0.5);
      while (l1Size > targetSize && l1Queue.length > 0) {
        const evictKey = l1Queue.shift();
        const old = l1Cache.get(evictKey);
        if (old) { l1Size -= old.length; l1Cache.delete(evictKey); }
      }
    }
  }
}

let redis = null;
if (REDIS_URL) {
  try {
    const Redis = require('ioredis');
    redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 2, retryStrategy: t => Math.min(t * 100, 3000) });
    redis.connect().catch(() => { redis = null; });
  } catch { redis = null; }
}

function getRedisKey(filePath) { return `dlcache:${filePath}`; }

const pendingReads = new Map();

function getFileBuffer(filePath, stat, cb) {
  const l1 = l1Get(filePath);
  if (l1) { cb(null, l1); return; }

  if (stat.size > MAX_CACHEABLE_FILE_SIZE) { cb(null, null); return; }

  if (redis && stat.size <= REDIS_CACHE_MAX_SIZE) {
    const rk = getRedisKey(filePath);
    redis.getBuffer(rk).then(val => {
      if (val) {
        l1Set(filePath, val);
        cb(null, val);
        return;
      }
      readFromDisk(filePath, stat, cb);
    }).catch(() => readFromDisk(filePath, stat, cb));
    return;
  }

  readFromDisk(filePath, stat, cb);
}

function readFromDisk(filePath, stat, cb) {
  if (stat.size > MAX_CACHEABLE_FILE_SIZE) { cb(null, null); return; }

  if (pendingReads.has(filePath)) {
    pendingReads.get(filePath).push(cb);
    return;
  }

  if (inFlightBufferBytes + stat.size > MAX_INFLIGHT_BUFFER_BYTES) { cb(null, null); return; }

  pendingReads.set(filePath, [cb]);
  inFlightBufferBytes += stat.size;
  fs.readFile(filePath, (err, data) => {
    inFlightBufferBytes = Math.max(0, inFlightBufferBytes - stat.size);
    const cbs = pendingReads.get(filePath) || [];
    pendingReads.delete(filePath);
    if (err) { cbs.forEach(c => c(err)); return; }
    l1Set(filePath, data);
    if (redis && data.length <= REDIS_CACHE_MAX_SIZE) {
      redis.set(getRedisKey(filePath), data, 'EX', 3600).catch(() => {});
    }
    cbs.forEach(c => c(null, data));
  });
}

function getDiskInfo() {
  try {
    const df = execSync('df -h /', { encoding: 'utf8' });
    const parts = df.trim().split('\n')[1].split(/\s+/);
    return { total: parts[1], used: parts[2], available: parts[3], usagePercent: parts[4] };
  } catch {
    return { total: 'N/A', used: 'N/A', available: 'N/A', usagePercent: 'N/A' };
  }
}

function getPublicDirSize(dir) {
  try {
    if (!fs.existsSync(dir)) return '0 B';
    return execSync(`du -sh "${dir}"`, { encoding: 'utf8' }).trim().split('\t')[0];
  } catch { return 'N/A'; }
}

function getPublicFileCount(dir) {
  try {
    if (!fs.existsSync(dir)) return 0;
    return fs.readdirSync(dir).filter(f => { try { return fs.statSync(path.join(dir, f)).isFile(); } catch { return false; } }).length;
  } catch { return 0; }
}

if (!fs.existsSync(PUBLIC_DIR)) { fs.mkdirSync(PUBLIC_DIR, { recursive: true }); }

const MIME_TYPES = {
  '.txt': 'text/plain', '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
  '.json': 'application/json', '.xml': 'application/xml', '.pdf': 'application/pdf',
  '.zip': 'application/zip', '.gz': 'application/gzip', '.rar': 'application/vnd.rar',
  '.7z': 'application/x-7z-compressed', '.tar': 'application/x-tar',
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif',
  '.webp': 'image/webp', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
  '.mp4': 'video/mp4', '.mp3': 'audio/mpeg',
  '.exe': 'application/octet-stream', '.msi': 'application/octet-stream',
  '.dmg': 'application/octet-stream', '.deb': 'application/octet-stream',
  '.rpm': 'application/octet-stream', '.AppImage': 'application/octet-stream',
};

const FORCE_DOWNLOAD_EXTENSIONS = new Set([
  '.exe', '.msi', '.dmg', '.zip', '.rar', '.7z', '.gz', '.tar',
  '.deb', '.rpm', '.AppImage', '.iso', '.bin'
]);

const rateLimitMap = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [ip, timestamps] of rateLimitMap) {
    const fresh = timestamps.filter(t => now - t < RATE_WINDOW);
    if (fresh.length === 0) rateLimitMap.delete(ip);
    else rateLimitMap.set(ip, fresh);
  }
}, RATE_WINDOW).unref();

function rateLimiter(req, res, next) {
  const ip = req.socket.remoteAddress;
  const now = Date.now();
  if (!rateLimitMap.has(ip)) rateLimitMap.set(ip, []);
  const timestamps = rateLimitMap.get(ip).filter(t => now - t < RATE_WINDOW);
  if (timestamps.length >= RATE_LIMIT) {
    res.writeHead(429, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('429 Too Many Requests');
    return;
  }
  timestamps.push(now);
  rateLimitMap.set(ip, timestamps);
  next();
}

function sanitizePath(requestPath) {
  let decoded;
  try { decoded = decodeURIComponent(requestPath); } catch { decoded = requestPath; }
  let normalized = path.normalize(decoded).replace(/\\/g, '/');
  normalized = normalized.split('?')[0].split('#')[0];
  while (normalized.startsWith('/')) normalized = normalized.slice(1);
  return normalized;
}

function sendSecurityHeaders(res, isDownload) {
  const headers = {
    'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'X-XSS-Protection': '0',
    'Referrer-Policy': 'no-referrer', 'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'Permissions-Policy': 'accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()',
    'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
    'Pragma': 'no-cache', 'Access-Control-Allow-Origin': '*',
  };
  if (isDownload) headers['Content-Security-Policy'] = "default-src 'none'; base-uri 'none'; form-action 'none'";
  Object.entries(headers).forEach(([k, v]) => { if (!res.headersSent) res.setHeader(k, v); });
}

function isPathInAllowedSubdir(requestPath) {
  for (const subdir of ALLOWED_SUBDIRS) {
    if (requestPath === subdir || requestPath.startsWith(subdir + '/')) return true;
  }
  return false;
}

function renderDirectoryListing(dirPath, relativePath, title, res) {
  let items;
  try { items = fs.readdirSync(dirPath); } catch {
    res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('500 Internal Server Error'); return;
  }
  const files = [], subdirs = [];
  const isRoot = !relativePath;
  for (const item of items) {
    const fullPath = path.join(dirPath, item);
    try {
      const stat = fs.statSync(fullPath);
      if (stat.isFile()) files.push(item);
      else if (stat.isDirectory() && isRoot && ALLOWED_SUBDIRS.includes(item)) subdirs.push(item);
    } catch {}
  }
  sendSecurityHeaders(res, false);
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  let html = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapeHtml(title)} - ${DOMAIN}</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Segoe UI',Tahoma,sans-serif; background:#0f0f0f; color:#e0e0e0; min-height:100vh; }
  .container { max-width:900px; margin:0 auto; padding:40px 20px; }
  h1 { text-align:center; margin-bottom:10px; font-weight:300; color:#fff; font-size:2em; letter-spacing:1px; }
  .subtitle { text-align:center; color:#888; margin-bottom:35px; font-size:.9em; }
  .breadcrumb { color:#888; margin-bottom:20px; font-size:.85em; text-align:center; }
  .breadcrumb a { color:#888; text-decoration:none; }
  .breadcrumb a:hover { color:#c8c8ff; }
  .item-grid { display:grid; gap:10px; }
  .item-row { display:flex; align-items:center; justify-content:space-between; background:#1a1a2e; border:1px solid #2a2a4a; padding:14px 20px; border-radius:8px; transition:background .2s,border-color .2s; }
  .item-row:hover { background:#1e1e3a; border-color:#4a4a8a; }
  .item-info { display:flex; align-items:center; gap:14px; overflow:hidden; }
  .item-icon { font-size:1.3em; flex-shrink:0; }
  .item-name { font-size:.95em; word-break:break-all; color:#c8c8ff; }
  .item-size { color:#888; font-size:.8em; flex-shrink:0; }
  .dl-btn { background:#2d2d5e; color:#fff; text-decoration:none; padding:8px 18px; border-radius:6px; font-size:.85em; transition:background .2s; flex-shrink:0; cursor:pointer; }
  .dl-btn:hover { background:#3d3d7e; }
  .empty { text-align:center; color:#666; padding:60px 0; font-size:1.1em; }
  .footer { text-align:center; color:#444; font-size:.75em; margin-top:40px; }
  @media (max-width:600px) { .item-row { flex-direction:column; align-items:stretch; gap:10px; } .dl-btn { text-align:center; } }
</style>
</head>
<body>
<div class="container">
<h1>${escapeHtml(title)}</h1>
<p class="subtitle">${DOMAIN}</p>
${isRoot ? '' : `<div class="breadcrumb"><a href="/">خانه</a>${relativePath.split('/').filter(Boolean).map((p,i,arr) => { const url = '/' + arr.slice(0,i+1).join('/'); return ` / <a href="${url}">${escapeHtml(p)}</a>`; }).join('')}</div>`}
<div class="item-grid">`;
  for (const dir of subdirs) {
    html += `<div class="item-row"><div class="item-info"><span class="item-icon">&#128193;</span><span class="item-name">${escapeHtml(dir)}</span><span class="item-size">پوشه</span></div><a class="dl-btn" href="/${encodeURIComponent(dir)}">مشاهده</a></div>`;
  }
  for (const file of files) {
    const stat = fs.statSync(path.join(dirPath, file));
    const size = formatSize(stat.size);
    const icon = getFileIcon(file);
    const href = isRoot ? `/${encodeURIComponent(file)}` : `/${relativePath}/${encodeURIComponent(file)}`;
    html += `<div class="item-row"><div class="item-info"><span class="item-icon">${icon}</span><span class="item-name">${escapeHtml(file)}</span><span class="item-size">${size}</span></div><a class="dl-btn" href="${href}" download>دانلود</a></div>`;
  }
  if (files.length === 0 && subdirs.length === 0) html += '<div class="empty">هیچ موردی وجود ندارد</div>';
  html += `</div><div class="footer">Secure Download Server</div></div></body></html>`;
  res.end(html);
}

function parseRange(rangeStr, fileSize) {
  const match = rangeStr.replace(/bytes=/, '').match(/^(\d*)-(\d*)$/);
  if (!match) return null;
  let start = match[1] !== '' ? parseInt(match[1]) : null;
  let end = match[2] !== '' ? parseInt(match[2]) : null;
  if (start === null && end === null) return null;
  if (start === null) { start = Math.max(0, fileSize - end); end = fileSize - 1; }
  if (end === null) end = fileSize - 1;
  if (start > end || start >= fileSize) return null;
  end = Math.min(end, fileSize - 1);
  return { start, end, length: end - start + 1 };
}

function serveFile(safePath, stat, mimeType, isDownload, range, req, res) {
  activeStreams++;
  let decremented = false;
  const done = () => { if (!decremented) { decremented = true; activeStreams = Math.max(0, activeStreams - 1); } };
  res.on('close', done);

  sendSecurityHeaders(res, isDownload);
  if (range) {
    res.writeHead(206, {
      'Content-Type': mimeType, 'Content-Length': range.length,
      'Content-Range': `bytes ${range.start}-${range.end}/${stat.size}`,
      'Content-Disposition': isDownload ? `attachment; filename="${encodeURIComponent(path.basename(safePath))}"` : `inline; filename="${encodeURIComponent(path.basename(safePath))}"`,
      'Accept-Ranges': 'bytes',
    });
  } else {
    res.writeHead(200, {
      'Content-Type': mimeType, 'Content-Length': stat.size,
      'Content-Disposition': isDownload ? `attachment; filename="${encodeURIComponent(path.basename(safePath))}"` : `inline; filename="${encodeURIComponent(path.basename(safePath))}"`,
      'Accept-Ranges': 'bytes',
    });
  }
  if (req.method === 'HEAD') { res.end(); return; }
  try { res.socket.setNoDelay(true); } catch {}
  getFileBuffer(safePath, stat, (err, buf) => {
    if (err) {
      if (!res.headersSent) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('500 Internal Server Error'); }
      return;
    }
    if (buf) {
      if (range) res.end(buf.slice(range.start, range.end + 1));
      else res.end(buf);
      return;
    }
    const streamOpts = range ? { start: range.start, end: range.end, highWaterMark: STREAM_HIGH_WATER_MARK } : { highWaterMark: STREAM_HIGH_WATER_MARK };
    const stream = fs.createReadStream(safePath, streamOpts);
    pipeline(stream, res, err => {
      if (err && !res.headersSent) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('500 Internal Server Error'); }
    });
  });
}

function handleRequest(req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('405 Method Not Allowed'); return;
  }
  let requestPath = sanitizePath(req.url);
  if (!requestPath) requestPath = '';
  if (requestPath === 'index.html') requestPath = '';
  if (!requestPath) { renderDirectoryListing(PUBLIC_DIR, '', 'دانلود فایل‌ها', res); return; }
  const firstSegment = requestPath.split('/')[0];
  const safePath = path.normalize(path.join(PUBLIC_DIR, requestPath));
  if (!safePath.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('403 Forbidden'); return; }

  fs.stat(safePath, (err, stat) => {
    if (err) {
      if (res.writableEnded) return;
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('404 Not Found'); return;
    }

    if (ALLOWED_SUBDIRS.length > 0) {
      const inAllowed = isPathInAllowedSubdir(requestPath);
      const pathIsFileInRoot = !requestPath.includes('/') && stat.isFile();

      if (stat.isDirectory()) {
        const cleanPath = requestPath.replace(/\/+$/, '');
        const isExactAllowed = !cleanPath.includes('/') && ALLOWED_SUBDIRS.includes(cleanPath);
        if (!isExactAllowed) {
          res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('403 Forbidden'); return;
        }
      }

      if (!inAllowed && !pathIsFileInRoot) {
        res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('403 Forbidden'); return;
      }
    }

    if (stat.isDirectory()) { renderDirectoryListing(safePath, requestPath, firstSegment, res); return; }
    if (MAX_FILE_SIZE > 0 && stat.size > MAX_FILE_SIZE) { res.writeHead(413, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('413 Payload Too Large'); return; }

    if (activeStreams >= MAX_CONCURRENT_STREAMS) {
      res.writeHead(503, { 'Content-Type': 'text/plain; charset=utf-8', 'Retry-After': '3' });
      res.end('503 Service Unavailable - ظرفیت سرور تکمیله، چند لحظه دیگه امتحان کنید');
      return;
    }

    const ext = path.extname(safePath).toLowerCase();
    const mimeType = MIME_TYPES[ext] || 'application/octet-stream';
    const isDownload = FORCE_DOWNLOAD_EXTENSIONS.has(ext) || mimeType === 'application/octet-stream';

    const rangeHeader = req.headers.range;
    let range = null;
    if (rangeHeader) {
      range = parseRange(rangeHeader, stat.size);
      if (!range) {
        res.writeHead(416, { 'Content-Range': `bytes */${stat.size}`, 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('416 Range Not Satisfiable'); return;
      }
    }

    serveFile(safePath, stat, mimeType, isDownload, range, req, res);
  });
}

function formatSize(bytes) {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
}

function getFileIcon(filename) {
  const ext = path.extname(filename).toLowerCase();
  const icons = {
    '.zip': '\u{1F4E6}', '.rar': '\u{1F4E6}', '.7z': '\u{1F4E6}', '.tar': '\u{1F4E6}', '.gz': '\u{1F4E6}',
    '.exe': '\u{2699}', '.msi': '\u{2699}', '.deb': '\u{2699}', '.rpm': '\u{2699}',
    '.dmg': '\u{1F4BF}', '.iso': '\u{1F4BF}', '.pdf': '\u{1F4C4}', '.txt': '\u{1F4DD}',
    '.jpg': '\u{1F5BC}', '.jpeg': '\u{1F5BC}', '.png': '\u{1F5BC}', '.gif': '\u{1F5BC}',
    '.mp4': '\u{1F3AC}', '.mp3': '\u{1F3B5}',
  };
  return icons[ext] || '\u{1F4CE}';
}

function escapeHtml(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function attachConnectionSafety(req, res) {
  req.on('error', () => { try { if (!res.headersSent) res.destroy(); } catch {} });
  res.on('error', () => { try { res.destroy(); } catch {} });
}

const requestHandler = (req, res) => {
  attachConnectionSafety(req, res);
  checkMemory();
  rateLimiter(req, res, () => handleRequest(req, res));
};

function createServer(handler) {
  const srv = http.createServer(handler);
  srv.requestTimeout = 30000;
  srv.headersTimeout = 8000;
  srv.keepAliveTimeout = 5000;
  srv.maxConnections = 512;
  attachServerSafety(srv);
  return srv;
}

function attachServerSafety(srv) {
  srv.on('connection', socket => { socket.on('error', () => {}); });
  srv.on('secureConnection', socket => { socket.on('error', () => {}); });
  srv.on('clientError', (err, socket) => {
    if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    else socket.destroy();
  });
  srv.on('tlsClientError', () => {});
  srv.on('error', err => { console.error(`  [!] Server error: ${err.message}`); });
}

function startWorker() {
  const tag = cluster.isWorker ? `Worker ${cluster.worker.id} | ` : '';
  if (cluster.isWorker) {
    process.on('uncaughtException', err => {
      console.error(`  [!] ${tag}Uncaught: ${err.stack || err.message}`);
      process.exit(1);
    });
    process.on('unhandledRejection', err => {
      console.error(`  [!] ${tag}UnhandledRejection: ${err && err.stack || err}`);
    });
    setInterval(checkMemory, 10000);
  }
  if (SSL_KEY && SSL_CERT && fs.existsSync(SSL_KEY) && fs.existsSync(SSL_CERT)) {
    const sslOptions = { key: fs.readFileSync(SSL_KEY), cert: fs.readFileSync(SSL_CERT) };
    const srv = https.createServer(sslOptions, requestHandler);
    srv.requestTimeout = 30000;
    srv.headersTimeout = 8000;
    srv.keepAliveTimeout = 5000;
    srv.maxConnections = 512;
    attachServerSafety(srv);
    srv.listen(PORT, 511, () => { console.log(`  ${tag}HTTPS : https://${DOMAIN}:${PORT}`); });
  } else { console.log(`  ${tag}SSL certs not found, HTTP only.`); }
  const httpSrv = createServer((req, res) => {
    if (fs.existsSync(SSL_KEY) && fs.existsSync(SSL_CERT)) { res.writeHead(301, { 'Location': `https://${req.headers.host || DOMAIN}${req.url}` }); res.end(); }
    else { requestHandler(req, res); }
  });
  httpSrv.listen(HTTP_PORT, 511, () => {
    const redirectMsg = !fs.existsSync(SSL_KEY) || !fs.existsSync(SSL_CERT) ? '' : ' (redirects to HTTPS)';
    console.log(`  ${tag}HTTP  : http://0.0.0.0:${HTTP_PORT}${redirectMsg}`);
  });
}

function printStartupInfo(label) {
  const disk = getDiskInfo();
  const publicSize = getPublicDirSize(PUBLIC_DIR);
  const publicFiles = getPublicFileCount(PUBLIC_DIR);
  const ramTotal = formatSize(TOTAL_RAM);
  const ramPerWorker = formatSize(PER_WORKER_RAM);
  const cachePerWorker = formatSize(PER_WORKER_CACHE);
  const cacheInfo = `${formatSize(l1Size)} / ${cachePerWorker}`;
  console.log('='.repeat(58));
  console.log(`  ${label}`);
  console.log('='.repeat(58));
  console.log(`  PID        : ${process.pid}`);
  console.log(`  Platform   : ${os.platform()} ${os.release()}`);
  console.log(`  RAM        : ${ramTotal}`);
  console.log(`  Domain     : ${DOMAIN}`);
  console.log(`  RAM Cache  : ${cacheInfo}${redis ? ' + Redis' : ''}`);
  console.log(`  Cacheable  : max ${formatSize(MAX_CACHEABLE_FILE_SIZE)}/file (bigger files always stream)`);
  console.log(`  Concurrent : max ${MAX_CONCURRENT_STREAMS} downloads/worker`);
  console.log('-'.repeat(58));
  console.log('  DISK:');
  console.log(`    Total    : ${disk.total} | Used: ${disk.used} | Free: ${disk.available}`);
  console.log('-'.repeat(58));
  console.log('  PUBLIC:');
  console.log(`    Path     : ${PUBLIC_DIR}`);
  console.log(`    Size     : ${publicSize} | Files: ${publicFiles}`);
  console.log('-'.repeat(58));
}

if (cluster.isMaster && NUM_WORKERS > 1) {
  cluster.setupMaster({ execArgv: [`--max-old-space-size=${WORKER_MAX_OLD_SPACE_MB}`] });
  printStartupInfo(`SECURE DOWNLOAD SERVER (${NUM_WORKERS} WORKERS)`);
  console.log(`  Master PID: ${process.pid}`);
  console.log(`  Workers   : ${NUM_WORKERS}`);
  console.log(`  CPU Cores : ${os.cpus().length}`);
  console.log(`  RAM Total : ${formatSize(TOTAL_RAM)}`);
  console.log(`  RAM/Worker: ${formatSize(PER_WORKER_RAM)} (cache: ${formatSize(PER_WORKER_CACHE)})`);
  console.log('-'.repeat(58));
  console.log('  Starting workers...');
  for (let i = 0; i < NUM_WORKERS; i++) cluster.fork();
  let restartDelay = 100;
  cluster.on('exit', (worker, code, signal) => {
    const isOOM = code === 1 || signal === 'SIGKILL';
    console.log(`  [!] Worker ${worker.id} died (code=${code}, signal=${signal})${isOOM ? ' [OOM]' : ''}. Restarting in ${restartDelay}ms...`);
    setTimeout(() => { cluster.fork(); restartDelay = Math.max(100, Math.min(restartDelay * 0.8, 5000)); }, restartDelay);
    if (isOOM) restartDelay = Math.min(restartDelay * 2, 10000);
  });
  console.log('-'.repeat(58));
  console.log(`  Master ${process.pid} running with ${NUM_WORKERS} workers`);
  console.log('='.repeat(58));
} else {
  if (NUM_WORKERS <= 1) printStartupInfo('SECURE DOWNLOAD SERVER');
  startWorker();
  if (cluster.isWorker) {
    console.log('-'.repeat(58));
    console.log(`  Worker ${cluster.worker.id} ready`);
    console.log('='.repeat(58));
  } else {
    console.log('-'.repeat(58));
    console.log(`  PID ${process.pid} ready`);
    console.log('='.repeat(58));
  }
}
