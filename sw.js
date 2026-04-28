// ══════════════════════════════════════════════════════════
// sw.js — 學習數據分析儀表板 Service Worker
// 策略：App Shell (Cache First) + data.json (Network First)
// ══════════════════════════════════════════════════════════

const CACHE_VERSION = 'la-dash-v1';
const DATA_CACHE    = 'la-dash-data-v1';

// App Shell：靜態資源，安裝時全部快取
// ⚠ CDN 資源釘定版本號，確保快取與 HTML 引用一致
const CHARTJS_URL = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js';

const APP_SHELL = [
  './index.html',
  './manifest.json',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './icons/icon-180.png',
  CHARTJS_URL,
];

// ── 安裝：快取 App Shell ──────────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
      .catch(err => console.warn('[SW] Install cache failed:', err))
  );
});

// ── 啟動：清除舊快取 ──────────────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_VERSION && key !== DATA_CACHE)
          .map(key => {
            console.log('[SW] Deleting old cache:', key);
            return caches.delete(key);
          })
      )
    ).then(() => self.clients.claim())
  );
});

// ── 攔截請求 ─────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;

  // 只處理 GET
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // data.json → Network First（確保每次取得最新資料）
  if (url.pathname.endsWith('data.json')) {
    event.respondWith(networkFirstData(request));
    return;
  }

  // Chart.js CDN → Cache First（版本已釘定，與 APP_SHELL 一致）
  if (url.href === CHARTJS_URL) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // Google Fonts → Cache First
  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(cacheFirst(request));
    return;
  }

  // App Shell 靜態資源 → Cache First
  if (url.pathname.match(/\.(html|js|css|png|svg|ico|webmanifest|json)$/)) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // 其他 → Network First
  event.respondWith(networkFirst(request));
});

// ── Cache First 策略 ──────────────────────────────────────
async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;

  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      await safePut(cache, request, response.clone());
    }
    return response;
  } catch {
    return new Response('離線中，此資源尚未快取', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// ── Network First（data.json 專用，帶離線回退）────────────
async function networkFirstData(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(DATA_CACHE);
      await safePut(cache, request, response.clone());
    }
    return response;
  } catch {
    // 離線：嘗試回傳上次快取的 data.json
    const cached = await caches.match(request, { cacheName: DATA_CACHE });
    if (cached) {
      console.log('[SW] Offline: serving cached data.json');
      return cached;
    }
    return new Response(JSON.stringify({
      error: 'offline',
      message: '目前離線且無快取資料，請連線後重新整理'
    }), {
      status: 503,
      headers: { 'Content-Type': 'application/json; charset=utf-8' }
    });
  }
}

// ── Network First 通用 ────────────────────────────────────
async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      await safePut(cache, request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response('離線中', { status: 503 });
  }
}

// ── 安全快取（避免 QuotaExceededError）───────────────────
async function safePut(cache, request, response) {
  try {
    await cache.put(request, response);
  } catch (e) {
    if (e.name === 'QuotaExceededError') {
      console.warn('[SW] Cache quota exceeded, pruning...');
      await pruneCache(cache);
    }
  }
}

async function pruneCache(cache) {
  const keys = await cache.keys();
  if (keys.length > 40) {
    const toDelete = keys.slice(0, keys.length - 40);
    await Promise.all(toDelete.map(k => cache.delete(k)));
  }
}

// ── 接收來自主頁面的訊息 ──────────────────────────────────
self.addEventListener('message', (event) => {
  if (event.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
  if (event.data?.type === 'GET_VERSION') {
    event.ports[0].postMessage({ version: CACHE_VERSION });
  }
});
