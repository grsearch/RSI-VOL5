'use strict';
// src/birdeye.js — Birdeye API 封装 (V4 — 合并缓存，大幅降低API消耗)
//
// 架构：
//   1. 主力：Birdeye WebSocket SUBSCRIBE_PRICE（1s OHLCV 推送，延迟 50-150ms）
//   2. 兜底：HTTP /defi/price（WS 断开时自动切换）
//   3. FDV + LP：/defi/token_overview 合并请求，共享缓存（默认5分钟TTL）
//      ★ 修复V3：getFdv 和 getLiquidity 各自独立请求 → 合并为单次请求
//      ★ 缓存从5秒提升到5分钟，30个币×1/min vs 30个币×12/min，减少约90%消耗
//
// B-05 级别支持 WebSocket + 100 token 并发订阅

const WebSocket = require('ws');
const fetch     = require('node-fetch');
const logger    = require('./logger');

const BIRDEYE_KEY  = process.env.BIRDEYE_API_KEY || '';
const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || '';
const BASE         = 'https://public-api.birdeye.so';
// FDV/LP 缓存时间，默认30分钟（可通过 FDV_CACHE_MS 环境变量调整）
// ★ V5: 从5分钟提升到30分钟，FDV变化不敏感，买入前会强制刷新
const FDV_CACHE_MS = parseInt(process.env.FDV_CACHE_MS || String(30 * 60 * 1000), 10);

// ── WebSocket 价格流 ──────────────────────────────────────────────

const WS_URL = `wss://public-api.birdeye.so/socket/solana?x-api-key=${BIRDEYE_KEY}`;
const PING_INTERVAL_MS  = 25000;
const RECONNECT_BASE_MS = 1000;
const MAX_RECONNECT_MS  = 30000;

class BirdeyePriceStream {
  constructor() {
    this._ws         = null;
    this._connected  = false;
    this._pingTimer  = null;
    this._retryCount = 0;
    this._stopping   = false;

    // address → { price, ts, callbacks: Set<fn> }
    this._subscriptions = new Map();
  }

  start() {
    if (!BIRDEYE_KEY) {
      logger.warn('[BirdeyeWS] ⚠️ BIRDEYE_API_KEY 未设置，仅用 HTTP 轮询');
      return;
    }
    this._stopping = false;
    this._connect();
  }

  stop() {
    this._stopping = true;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
    this._connected = false;
  }

  subscribe(address, onPrice) {
    if (!this._subscriptions.has(address)) {
      this._subscriptions.set(address, { price: null, ts: 0, callbacks: new Set() });
    }
    this._subscriptions.get(address).callbacks.add(onPrice);

    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendSubscribe(address);
    }

    logger.info('[BirdeyeWS] 📌 订阅价格 %s，当前 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  unsubscribe(address) {
    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendUnsubscribe(address);
    }
    this._subscriptions.delete(address);
    logger.info('[BirdeyeWS] 🔕 取消价格订阅 %s，剩余 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  getCachedPrice(address) {
    const sub = this._subscriptions.get(address);
    if (!sub || !sub.price || Date.now() - sub.ts > 10000) return null;
    return sub.price;
  }

  isConnected() { return this._connected; }

  _connect() {
    if (this._stopping) return;

    const safeUrl = WS_URL.replace(/x-api-key=[^&]+/, 'x-api-key=***');
    logger.info('[BirdeyeWS] 连接 %s ...', safeUrl);

    this._ws = new WebSocket(WS_URL);

    this._ws.on('open', () => {
      logger.info('[BirdeyeWS] ✅ WebSocket 已连接');
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_INTERVAL_MS);

      for (const address of this._subscriptions.keys()) {
        this._sendSubscribe(address);
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});

    this._ws.on('error', (err) => {
      logger.error('[BirdeyeWS] 错误: %s', err.message);
    });

    this._ws.on('close', () => {
      logger.warn('[BirdeyeWS] 连接关闭');
      this._connected = false;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (!this._stopping) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_BASE_MS * Math.pow(1.5, this._retryCount - 1), MAX_RECONNECT_MS);
        logger.info('[BirdeyeWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => this._connect(), delay);
      }
    });
  }

  _sendSubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'SUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
    logger.debug('[BirdeyeWS] 📡 SUBSCRIBE_PRICE %s', address.slice(0, 8) + '...');
  }

  _sendUnsubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'UNSUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
  }

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    if (msg.type === 'PRICE_DATA' && msg.data) {
      const { address } = msg.data;
      const close = parseFloat(msg.data.c);
      if (!address || !Number.isFinite(close) || close <= 0) return;

      const sub = this._subscriptions.get(address);
      if (!sub) return;

      const now = Date.now();
      sub.price = close;
      sub.ts    = now;

      const ohlcv = {
        open:   parseFloat(msg.data.o || close),
        high:   parseFloat(msg.data.h || close),
        low:    parseFloat(msg.data.l || close),
        volume: parseFloat(msg.data.v || 0),
      };

      for (const cb of sub.callbacks) {
        try { cb(close, now, ohlcv); } catch (err) {
          logger.error('[BirdeyeWS] 价格回调错误: %s', err.message);
        }
      }
    }
  }
}

// 单例
const priceStream = new BirdeyePriceStream();

// ── FDV + Liquidity 合并缓存 ─────────────────────────────────────
// ★ getFdv 和 getLiquidity 共享同一个 token_overview 请求 + 缓存
// ★ 默认缓存5分钟，30个币每分钟只请求1次（原来每5秒1次，减少60倍消耗）

const _overviewCache = new Map(); // address → { fdv, liquidity, ts }

// 从时间戳值中提取毫秒（自动判断秒/毫秒）
function _toMs(val) {
  if (!val || isNaN(val)) return null;
  const n = Number(val);
  if (!isFinite(n) || n <= 0) return null;
  return n > 1e12 ? n : n * 1000; // 10位=秒级，13位=毫秒级
}

// 从 token_overview data 中提取 createdAt（尝试所有已知字段名）
function _extractCreatedAt(data) {
  // Birdeye 各版本字段名整理（按可能性排序）
  const candidates = [
    data.createdAt,
    data.createAt,
    data.creationTime,
    data.mintTime,
    data.firstMintTime,
    data.mint_time,
    data.listingTime,
    data.listing_time,
    data.liquidityAddedAt,     // WS TOKEN_NEW_LISTING 里见过这个字段
    data.firstAddLiquidityTime,
    data.extensions?.createdAt,
    data.extensions?.creationTime,
    data.extensions?.mintTime,
    data.extensions?.liquidityAddedAt,
  ];
  for (const v of candidates) {
    const ms = _toMs(v);
    if (ms && ms > 1000000000000) return ms; // 合理范围：2001年以后
  }
  return null;
}

// 打印一次完整 data 用于调试 createdAt 字段名（只打一次避免刷屏）
let _overviewDataLogged = false;
function _logOverviewData(address, data) {
  if (_overviewDataLogged) return;
  _overviewDataLogged = true;
  logger.warn('[Birdeye] ===== overview 完整字段 (调试用，仅此一次) =====');
  logger.warn('[Birdeye] 地址: %s', address);
  logger.warn('[Birdeye] keys: %s', JSON.stringify(Object.keys(data)));
  // 打印所有值不为空且类型为number的字段（可能是时间戳）
  const numFields = {};
  for (const [k, v] of Object.entries(data)) {
    if (typeof v === 'number' && v > 1000000000) numFields[k] = v;
  }
  logger.warn('[Birdeye] 数值型字段(可能含时间戳): %s', JSON.stringify(numFields));
  logger.warn('[Birdeye] ================================================');
}

async function _fetchOverview(address) {
  const cached = _overviewCache.get(address);
  if (cached && Date.now() - cached.ts < FDV_CACHE_MS) return cached;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const url = `${BASE}/defi/token_overview?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) {
      logger.warn('[Birdeye] token_overview %s 返回 %d', address, res.status);
      return cached || null;
    }
    const json = await res.json();
    const data = json?.data || {};

    let createdAt = _extractCreatedAt(data);

    const entry = {
      fdv:       data.fdv ?? data.mc ?? null,
      liquidity: data.liquidity ?? data.lp ?? null,
      createdAt,
      ts:        Date.now(),
    };

    // 保留旧缓存中的 createdAt（不会变）
    if (!entry.createdAt && cached?.createdAt) entry.createdAt = cached.createdAt;

    // createdAt 仍未找到 → 打印调试信息 + 尝试 meta-data/single 接口
    if (!entry.createdAt) {
      _logOverviewData(address, data);
      logger.warn('[Birdeye] overview %s 无createdAt，尝试meta-data接口',
        address.slice(0, 8));
      try {
        const metaTs = await _fetchMetaCreatedAt(address);
        if (metaTs) entry.createdAt = metaTs;
      } catch (_) {}
    }

    if (entry.createdAt) {
      logger.debug('[Birdeye] %s age=%sh',
        address.slice(0, 8), Math.round((Date.now() - entry.createdAt) / 3600000));
    }

    _overviewCache.set(address, entry);
    return entry;
  } catch (err) {
    logger.warn('[Birdeye] _fetchOverview %s 失败: %s', address, err.message);
    return cached || null;
  } finally {
    clearTimeout(timeout);
  }
}

// 备用：从 Helius getAsset 获取代币创建时间（比 Birdeye 更可靠）
async function _fetchMetaCreatedAt(address) {
  if (!HELIUS_RPC_URL) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const res = await fetch(HELIUS_RPC_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 'get-asset-age',
        method: 'getAsset',
        params: { id: address },
      }),
      signal: controller.signal,
    });
    if (!res.ok) return null;
    const json = await res.json();
    const result = json?.result || {};

    // getAsset 返回的创建时间字段
    const candidates = [
      result.createdAt,
      result.created_at,
      result.mint_extensions?.permanentDelegate?.delegate,  // 不是时间戳，跳过
      result.token_info?.price_info?.currency,              // 不是时间戳，跳过
    ];

    // Helius getAsset 里 createdAt 是 Unix 秒级时间戳
    for (const v of [result.createdAt, result.created_at]) {
      const ms = _toMs(v);
      if (ms && ms > 1000000000000) {
        logger.debug('[Birdeye] Helius getAsset %s createdAt=%s',
          address.slice(0, 8), new Date(ms).toISOString());
        return ms;
      }
    }

    // 如果 getAsset 没有 createdAt，尝试从 content.metadata 里找
    const metadata = result.content?.metadata || result.content || {};
    for (const v of [metadata.createdAt, metadata.created_at, metadata.mint_time]) {
      const ms = _toMs(v);
      if (ms && ms > 1000000000000) return ms;
    }

    return null;
  } catch (_) {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

// ── HTTP 兜底价格查询 ─────────────────────────────────────────────
// ★ 双层缓存：WS缓存（10s）→ HTTP本地缓存（30s）→ 真实HTTP请求
// 避免BirdeyeWS推送不活跃的币（低流动性）每秒发HTTP

const _priceHttpCache = new Map(); // address → { price, ts }
const PRICE_HTTP_CACHE_MS = parseInt(process.env.PRICE_HTTP_CACHE_MS || '60000', 10); // ★ V5: 默认60秒

async function getPrice(address) {
  // 1. 优先用 WS 缓存（最新，10秒有效）
  const wsCached = priceStream.getCachedPrice(address);
  if (wsCached !== null) return wsCached;

  // 2. HTTP 本地缓存（30秒，防止低流动性币每秒发请求）
  const httpCached = _priceHttpCache.get(address);
  if (httpCached && Date.now() - httpCached.ts < PRICE_HTTP_CACHE_MS) {
    return httpCached.price;
  }

  // 3. 真实 HTTP 请求
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const url = `${BASE}/defi/price?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`Birdeye price error: ${res.status}`);
    const json = await res.json();
    if (!json.success || !json.data) throw new Error('Birdeye price 返回异常');
    const price = json.data.value;
    _priceHttpCache.set(address, { price, ts: Date.now() });
    return price;
  } finally {
    clearTimeout(timeout);
  }
}

async function getFdv(address) {
  const entry = await _fetchOverview(address);
  return entry?.fdv ?? null;
}

/** 只读内存缓存，不发 HTTP 请求。缓存未命中时返回 null。
 *  用于主轮询的 FDV 监控：命中则检查，未命中则跳过（等买入前再刷）。*/
function getCachedFdv(address) {
  const cached = _overviewCache.get(address);
  if (!cached) return null;
  // 缓存已过期也返回旧值（宁可用旧值检查，也不跳过）
  return cached.fdv ?? null;
}

/** 强制绕过缓存，发 HTTP 请求获取最新 FDV。仅在买入前调用一次。*/
async function getFdvFresh(address) {
  _overviewCache.delete(address);          // 清除旧缓存，强制重新拉取
  const entry = await _fetchOverview(address);
  return entry?.fdv ?? null;
}

async function getLiquidity(address) {
  const entry = await _fetchOverview(address);
  return entry?.liquidity ?? null;
}

function clearCache(address) {
  _overviewCache.delete(address);
  _priceHttpCache.delete(address);
}

/** 获取完整 overview（fdv + lp + createdAt），走缓存 */
async function getOverview(address) {
  return await _fetchOverview(address);
}

// ── 历史 OHLCV K 线拉取 ──────────────────────────────────────────
// Birdeye /defi/ohlcv 接口，返回历史 K 线数据
// type 映射：秒数 → Birdeye type 字符串
const KLINE_TYPE_MAP = {
  60:    '1m',
  180:   '3m',
  300:   '5m',
  900:   '15m',
  1800:  '30m',
  3600:  '1H',
  7200:  '2H',
  14400: '4H',
  21600: '6H',
  28800: '8H',
  43200: '12H',
  86400: '1D',
};

/**
 * 拉取历史 K 线，返回 candle 数组（可直接合并进 closedCandles）
 * @param {string} address - 代币地址
 * @param {number} intervalSec - K 线宽度（秒），如 300 = 5分钟
 * @param {number} bars - 需要拉取的 K 线根数，如 150
 * @returns {Array} candles - [{ openTime, closeTime, open, high, low, close, volume, buyVolume, sellVolume }]
 */
async function getOHLCV(address, intervalSec, bars = 150) {
  const type = KLINE_TYPE_MAP[intervalSec];
  if (!type) {
    // 没有精确匹配，找最接近的
    const keys = Object.keys(KLINE_TYPE_MAP).map(Number).sort((a, b) => a - b);
    const closest = keys.reduce((prev, curr) =>
      Math.abs(curr - intervalSec) < Math.abs(prev - intervalSec) ? curr : prev
    );
    logger.warn('[Birdeye] getOHLCV: %ds 无对应类型，使用 %ds (%s)', intervalSec, closest, KLINE_TYPE_MAP[closest]);
    return getOHLCV(address, closest, bars);
  }

  const now       = Math.floor(Date.now() / 1000);
  const time_from = now - intervalSec * (bars + 5); // 多拉5根，确保有足够数据
  const time_to   = now;

  const url = `${BASE}/defi/ohlcv?address=${address}&type=${type}&time_from=${time_from}&time_to=${time_to}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  try {
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) {
      logger.warn('[Birdeye] getOHLCV %s 返回 %d', address.slice(0, 8), res.status);
      return [];
    }
    const json = await res.json();
    const items = json?.data?.items || [];
    if (items.length === 0) {
      logger.warn('[Birdeye] getOHLCV %s 无数据', address.slice(0, 8));
      return [];
    }

    // 转换为系统 candle 格式
    const candles = items.map(item => {
      const openTime  = item.unixTime * 1000;          // 秒 → 毫秒
      const closeTime = openTime + intervalSec * 1000;
      return {
        openTime,
        closeTime,
        open:       item.o,
        high:       item.h,
        low:        item.l,
        close:      item.c,
        volume:     item.v || 0,     // Birdeye OHLCV 的 v 是总量
        buyVolume:  0,               // 历史数据无买卖方向分离，置0
        sellVolume: 0,
        tickCount:  1,
        priceTickCount: 1,
        fromHistory: true,           // 标记为历史数据
      };
    });

    // 按时间升序排列，去掉最后一根（可能未收盘）
    candles.sort((a, b) => a.openTime - b.openTime);
    const closed = candles.slice(0, -1); // 去掉最后一根未收盘K线

    logger.info('[Birdeye] getOHLCV %s type=%s 拉取 %d 根历史K线 (请求%d根)',
      address.slice(0, 8) + '...', type, closed.length, bars);
    return closed;
  } catch (err) {
    logger.warn('[Birdeye] getOHLCV %s 失败: %s', address.slice(0, 8), err.message);
    return [];
  } finally {
    clearTimeout(timeout);
  }
}

module.exports = { getPrice, getFdv, getCachedFdv, getFdvFresh, getLiquidity, getOverview, clearCache, priceStream, getOHLCV };
