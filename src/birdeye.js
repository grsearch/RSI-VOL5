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

async function _fetchOverview(address) {
  const cached = _overviewCache.get(address);
  if (cached && Date.now() - cached.ts < FDV_CACHE_MS) return cached;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
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
    const entry = {
      fdv:       data.fdv ?? data.mc ?? null,
      liquidity: data.liquidity ?? data.lp ?? null,
      ts:        Date.now(),
    };
    _overviewCache.set(address, entry);
    return entry;
  } catch (err) {
    logger.warn('[Birdeye] _fetchOverview %s 失败: %s', address, err.message);
    return cached || null;
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

module.exports = { getPrice, getFdv, getCachedFdv, getFdvFresh, getLiquidity, clearCache, priceStream };
