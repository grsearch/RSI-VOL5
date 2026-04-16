'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听
//
// V4 — 自动模式（auto）：根据代币数量动态切换订阅策略
//
// ★ 模式 "auto"（默认）：
//   - 代币数 ≤ HELIUS_TOKEN_LIMIT（默认15）→ token 精准模式
//   - 代币数 > HELIUS_TOKEN_LIMIT           → 自动升级到 pump 单订阅
//   - 升级后添加/移除代币无需重新订阅，无限扩展
//
// ★ 模式 "token"（手动强制）：
//   每个 token 独立 transactionSubscribe，credits 最省，适合 ≤15 个代币
//
// ★ 模式 "pump"（手动强制）：
//   一个 transactionSubscribe(accountInclude: [PumpAMM])，全量过滤
//   适合大量代币或频繁增删代币的场景
//
// credits 对比（监控 5 个 Pump AMM token）：
//   "token" 模式：~10 笔/秒 × ~0.5KB ≈ 100 credits/s ≈ 860万/天
//   "pump"  模式：~80 笔/秒 × ~0.5KB ≈ 800 credits/s ≈ 6900万/天

const WebSocket = require('ws');
const logger    = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

const HELIUS_WSS_URL         = process.env.HELIUS_WSS_URL || '';
const HELIUS_GATEKEEPER_URL  = process.env.HELIUS_GATEKEEPER_URL || '';
const HELIUS_API_KEY         = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL         = process.env.HELIUS_RPC_URL || '';

// "auto" | "token" | "pump"
// auto = 代币数 ≤ HELIUS_TOKEN_LIMIT 用 token 模式，超过自动切换 pump
const CFG_SUB_MODE    = (process.env.HELIUS_SUB_MODE || 'auto').toLowerCase();
// auto 模式下，超过此数量自动切换到 pump 单订阅
const TOKEN_LIMIT     = parseInt(process.env.HELIUS_TOKEN_LIMIT || '15', 10);

function getWsUrl() {
  if (HELIUS_GATEKEEPER_URL) {
    let url = HELIUS_GATEKEEPER_URL;
    if (url.startsWith('https://')) url = url.replace('https://', 'wss://');
    if (!url.startsWith('wss://')) url = `wss://${url}`;
    return { url, type: 'gatekeeper' };
  }
  if (HELIUS_WSS_URL) {
    return { url: HELIUS_WSS_URL, type: 'enhanced' };
  }
  const apiKey = HELIUS_API_KEY || extractApiKey(HELIUS_RPC_URL);
  if (!apiKey) return { url: '', type: 'none' };
  return { url: `wss://mainnet.helius-rpc.com/?api-key=${apiKey}`, type: 'enhanced' };
}

function extractApiKey(rpcUrl) {
  const m = (rpcUrl || '').match(/api-key=([a-f0-9-]+)/i);
  return m ? m[1] : '';
}

const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

const LAMPORTS     = 1e9;
const PING_MS      = 25000;
const RECONNECT_MS = 2000;
const MAX_RETRIES  = 999;

// ── HeliusTradeStream ───────────────────────────────────────────

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._connType    = 'none';

    // token → { symbol, onTrade, subId, rpcId }
    this._tokens = new Map();

    // rpcId → tokenAddress | '__pump__'（用于匹配订阅确认）
    this._pendingSubs = new Map();
    this._nextRpcId = 100;

    // pump 模式的 subId
    this._pumpSubId = null;

    // ★ 实际激活的模式（auto模式下动态变化）
    // 初始值根据配置决定：手动指定 pump/token 则固定，auto 则从 token 开始
    this._activeMode = CFG_SUB_MODE === 'pump' ? 'pump' : 'token';

    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0, txSkipped: 0, connType: 'none', subMode: this._activeMode };
  }

  // ── 当前是否使用 pump 模式 ──────────────────────────────────
  _isPumpMode() { return this._activeMode === 'pump'; }

  // ── 生命周期 ────────────────────────────────────────────────

  start() {
    const { url, type } = getWsUrl();
    if (!url) {
      logger.warn('[HeliusWS] ⚠️ 未配置 Helius WebSocket URL，链上量能数据不可用');
      return;
    }
    this._connType = type;
    this._stats.connType = type;
    logger.info('[HeliusWS] 配置模式: %s (当前激活: %s, 代币上限: %d)',
      CFG_SUB_MODE, this._activeMode, TOKEN_LIMIT);
    this._connect(url);
  }

  stop() {
    this._connected = false;
    this._retryCount = MAX_RETRIES + 1;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
  }

  // ── 连接管理 ────────────────────────────────────────────────

  _connect(wsUrl) {
    const safeUrl = wsUrl.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[HeliusWS] 连接 %s (类型: %s) ...', safeUrl, this._connType);

    this._ws = new WebSocket(wsUrl);

    this._ws.on('open', () => {
      logger.info('[HeliusWS] ✅ %s WebSocket 已连接 (模式: %s)', this._connType.toUpperCase(), this._activeMode);
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_MS);

      // 重连后恢复订阅（用当前激活的模式）
      if (this._isPumpMode()) {
        this._subscribePumpAmm();
      } else {
        // ★ 按顺序延迟恢复订阅，每个间隔150ms，避免瞬间大量请求压垮连接
        let i = 0;
        for (const [address, info] of this._tokens.entries()) {
          info.subId = null;
          const delay = i * 150;
          setTimeout(() => {
            if (this._tokens.has(address) && this._connected && !this._isPumpMode()) {
              this._subscribeToken(address);
            }
          }, delay);
          i++;
        }
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});
    this._ws.on('error', (err) => logger.error('[HeliusWS] 错误: %s', err.message));

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected = false;
      this._pendingSubs.clear();
      this._pumpSubId = null;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (this._retryCount < MAX_RETRIES) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_MS * Math.pow(1.5, this._retryCount - 1), 30000);
        logger.info('[HeliusWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => {
          const { url } = getWsUrl();
          if (url) this._connect(url);
        }, delay);
      }
    });
  }

  // ── 订阅：Pump AMM 模式 ───────────────────────────────────

  _subscribePumpAmm() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, '__pump__');

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [PUMP_AMM_PROGRAM], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));
    logger.info('[HeliusWS] 📡 订阅 Pump AMM program (单订阅模式)');
  }

  // ── 订阅：按 Token 精准模式 ───────────────────────────────

  _subscribeToken(tokenAddress) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, tokenAddress);

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [tokenAddress], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));

    const info = this._tokens.get(tokenAddress);
    if (info) info.rpcId = rpcId;
    logger.info('[HeliusWS] 📡 订阅 token %s (%s)',
      info?.symbol || '?', tokenAddress.slice(0, 8) + '...');
  }

  _unsubscribeToken(tokenAddress) {
    const info = this._tokens.get(tokenAddress);
    if (!info?.subId) return;

    if (this._ws?.readyState === WebSocket.OPEN) {
      const rpcId = this._nextRpcId++;
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: rpcId,
        method: 'transactionUnsubscribe',
        params: [info.subId],
      }));
      logger.info('[HeliusWS] 🔕 取消订阅 %s subId=%s', tokenAddress.slice(0, 8) + '...', info.subId);
    }
    info.subId = null;
  }

  // ── 外部接口 ──────────────────────────────────────────────

  subscribe(tokenAddress, symbol, onTrade) {
    this._tokens.set(tokenAddress, { symbol, onTrade, subId: null, rpcId: null });
    const count = this._tokens.size;

    // ★ auto 模式：检查是否需要升级到 pump 单订阅
    if (CFG_SUB_MODE === 'auto' && this._activeMode === 'token' && count > TOKEN_LIMIT) {
      logger.info('[HeliusWS] 🔄 代币数 %d > 上限 %d，自动切换到 pump 单订阅模式', count, TOKEN_LIMIT);
      this._upgradeToPump();
      // 升级后 pump 模式已覆盖所有代币，直接返回
      logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个 (模式=pump[auto升级])',
        symbol, tokenAddress.slice(0, 8) + '...', count);
      return;
    }

    if (!this._isPumpMode() && this._connected) {
      // token 模式：发送独立订阅（稍微延迟，避免连发）
      setTimeout(() => {
        if (this._tokens.has(tokenAddress) && this._connected && !this._isPumpMode()) {
          this._subscribeToken(tokenAddress);
        }
      }, 50);
    }
    // pump 模式：全局订阅已覆盖，无需额外操作

    logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个 (模式=%s)',
      symbol, tokenAddress.slice(0, 8) + '...', count, this._activeMode);
  }

  unsubscribe(tokenAddress) {
    // token 模式：取消独立订阅（pump模式不需要，全局订阅覆盖）
    if (!this._isPumpMode()) {
      this._unsubscribeToken(tokenAddress);
    }
    this._tokens.delete(tokenAddress);
    logger.info('[HeliusWS] 🔕 移除 token %s，剩余 %d 个 (模式=%s)',
      tokenAddress.slice(0, 8) + '...', this._tokens.size, this._activeMode);
  }

  // ── auto 模式：token → pump 升级 ──────────────────────────
  // 取消所有独立订阅，发起一个全局 pump 订阅
  _upgradeToPump() {
    if (this._activeMode === 'pump') return;
    this._activeMode = 'pump';
    this._stats.subMode = 'pump';

    if (!this._connected || this._ws?.readyState !== WebSocket.OPEN) return;

    // 取消所有已建立的 token 独立订阅
    for (const [addr, info] of this._tokens.entries()) {
      if (info.subId) {
        const rpcId = this._nextRpcId++;
        this._ws.send(JSON.stringify({
          jsonrpc: '2.0', id: rpcId,
          method: 'transactionUnsubscribe',
          params: [info.subId],
        }));
        info.subId = null;
      }
    }

    // 发起 pump AMM 全局订阅
    this._subscribePumpAmm();
  }

  // ── 消息处理 ──────────────────────────────────────────────

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    // 订阅确认
    if (msg.id && msg.result !== undefined) {
      const key = this._pendingSubs.get(msg.id);
      if (key === '__pump__') {
        this._pumpSubId = msg.result;
        this._pendingSubs.delete(msg.id);
        logger.info('[HeliusWS] ✅ Pump AMM 订阅确认 subId=%s', msg.result);
      } else if (key) {
        this._pendingSubs.delete(msg.id);
        const info = this._tokens.get(key);
        if (info) {
          info.subId = msg.result;
          logger.info('[HeliusWS] ✅ 订阅确认 %s (%s) subId=%s',
            info.symbol, key.slice(0, 8) + '...', msg.result);
        }
      }
      return;
    }

    // 交易通知
    if (msg.method === 'transactionNotification' && msg.params?.result) {
      this._stats.txReceived++;
      this._parseTransaction(msg.params.result, msg.params.subscription);
    }
  }

  // ── 交易解析 ──────────────────────────────────────────────

  _parseTransaction(result, subscriptionId) {
    try {
      const { transaction: txWrapper, signature } = result;
      if (!txWrapper) return;

      const meta = txWrapper.meta;
      const txData = txWrapper.transaction;
      if (!meta || meta.err) return;

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

      if (this._isPumpMode()) {
        // ── pump 模式：从交易中提取 mint，匹配监控列表 ──
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        let matched = false;

        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;

          matched = true;
          this._stats.txMatched++;
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.onTrade(trade);
          }
        }

        if (!matched) this._stats.txSkipped++;
      } else {
        // ── token 模式：通过 subscriptionId 精准匹配 ──
        let targetToken = null;
        for (const [addr, info] of this._tokens.entries()) {
          if (info.subId === subscriptionId) {
            targetToken = { address: addr, ...info };
            break;
          }
        }

        if (targetToken) {
          this._stats.txMatched++;
          const trade = this._extractTrade(targetToken.address, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            targetToken.onTrade(trade);
          }
          return;
        }

        // 兜底：subscriptionId 匹配不到时用 mint 匹配
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;
          this._stats.txMatched++;
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.onTrade(trade);
          }
        }
      }
    } catch (err) {
      logger.debug('[HeliusWS] 解析交易失败: %s', err.message);
    }
  }

  _extractTrade(tokenAddress, meta, txData, signature) {
    const preTokenBals  = meta.preTokenBalances  || [];
    const postTokenBals = meta.postTokenBalances  || [];
    const preBalances   = meta.preBalances  || [];
    const postBalances  = meta.postBalances || [];

    let accountKeys = [];
    if (txData?.message?.accountKeys) {
      accountKeys = txData.message.accountKeys.map(k =>
        typeof k === 'string' ? k : k.pubkey
      );
    }

    const postEntries = postTokenBals.filter(b => b.mint === tokenAddress);
    const preEntries  = preTokenBals.filter(b => b.mint === tokenAddress);
    if (postEntries.length === 0) return null;

    for (const postEntry of postEntries) {
      const owner = postEntry.owner;
      if (!owner) continue;

      const ownerIndex = accountKeys.indexOf(owner);
      if (ownerIndex < 0 || ownerIndex >= preBalances.length) continue;

      const preEntry = preEntries.find(
        b => b.accountIndex === postEntry.accountIndex || b.owner === owner
      );

      const postAmt = parseFloat(postEntry.uiTokenAmount?.uiAmount ?? '0');
      const preAmt  = preEntry ? parseFloat(preEntry.uiTokenAmount?.uiAmount ?? '0') : 0;
      const tokenDelta = postAmt - preAmt;
      if (Math.abs(tokenDelta) < 1e-12) continue;

      const solDelta = (postBalances[ownerIndex] - preBalances[ownerIndex]) / LAMPORTS;
      const isBuy  = tokenDelta > 0 && solDelta < 0;
      const isSell = tokenDelta < 0 && solDelta > 0;
      if (!isBuy && !isSell) continue;

      return {
        ts: Date.now(),
        signature,
        tokenAddress,
        owner,
        isBuy,
        solAmount:   Math.abs(solDelta),
        tokenAmount: Math.abs(tokenDelta),
        priceSol:    Math.abs(tokenDelta) > 0 ? Math.abs(solDelta) / Math.abs(tokenDelta) : 0,
      };
    }
    return null;
  }

  // ── 状态查询 ──────────────────────────────────────────────

  isConnected() { return this._connected; }
  getSubscriptionCount() { return this._tokens.size; }

  getStats() {
    let confirmedSubs = 0;
    for (const info of this._tokens.values()) {
      if (info.subId) confirmedSubs++;
    }
    return {
      connected:     this._connected,
      connType:      this._connType,
      subMode:       this._activeMode,
      tokens:        this._tokens.size,
      confirmedSubs: this._isPumpMode() ? (this._pumpSubId ? 1 : 0) : confirmedSubs,
      retryCount:    this._retryCount,
      ...this._stats,
    };
  }
}

const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
