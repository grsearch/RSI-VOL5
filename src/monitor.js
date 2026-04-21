'use strict';
// src/monitor.js — 核心监控引擎 V4
//
// V4 改进：
//   1. 去掉监控期限制和最大交易次数限制，代币持续监控直到手动移除
//   2. K线改为1分钟，止损轮询改为1分钟（可配置 SL_POLL_SEC）
//   3. 支持手动添加/删除代币
//   4. 卖出后不退出监控，重置状态等待下一个买入信号
//
// 交易生命周期：
//   addToken → [BUY → SELL → 冷却 → BUY → SELL → ...] → 手动移除 → removeToken

const EventEmitter = require('events');
const { evaluateSignal, buildCandles, filterValidCandles, checkStopLoss,
        calcRSIWithState, stepRSI,
        TRAILING_STOP_ENABLED, TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT } = require('./rsi');

// RSI 卖出阈值（从 CONFIG 取，与 rsi.js 保持一致）
const { CONFIG: RSI_CONFIG } = require('./rsi');
const _RSI_SELL  = RSI_CONFIG.RSI_SELL;
const _RSI_PANIC = RSI_CONFIG.RSI_PANIC;
const trader    = require('./trader');
const birdeye   = require('./birdeye');
const HIST_BARS = parseInt(process.env.HIST_BARS || '150', 10); // 启动时拉取的历史K线根数
const logger    = require('./logger');
const wsHub     = require('./wsHub');
const dataStore = require('./dataStore');
const heliusWs  = require('./heliusWs');

const FDV_EXIT          = parseFloat(process.env.FDV_EXIT_USD        || '30000');  // ★ V5: 改为3万
const LP_EXIT           = parseFloat(process.env.LP_EXIT_USD         || '10000');  // ★ V5: LP<1万退出
const POLL_SEC          = parseInt(process.env.PRICE_POLL_SEC        || '1',  10);
const KLINE_SEC         = parseInt(process.env.KLINE_INTERVAL_SEC    || '300', 10);
const DRY_RUN           = (process.env.DRY_RUN || 'false') === 'true';
const TRADE_SOL         = parseFloat(process.env.TRADE_SIZE_SOL      || '0.2');
const SELL_COOLDOWN_SEC = parseInt(process.env.SELL_COOLDOWN_SEC     || '1800', 10); // 默认30分钟
const SL_POLL_SEC       = parseInt(process.env.SL_POLL_SEC           || '60', 10);
const MAX_TOKENS        = parseInt(process.env.MAX_MONITOR_TOKENS    || '95', 10);  // ★ V5: 最大监控数
const OVERVIEW_PATROL_SEC = parseInt(process.env.OVERVIEW_PATROL_SEC || '7200', 10); // ★ V5: FDV/LP巡检间隔(秒)

// 全局交易记录
const _allTradeRecords = [];

function _loadPersistedTrades() {
  try {
    const trades = dataStore.loadTrades();
    const cutoff = Date.now() - 24 * 3600 * 1000;
    trades.filter(r => r.buyAt > cutoff).forEach(r => _allTradeRecords.push(r));
    if (_allTradeRecords.length > 0) {
      logger.info('[Monitor] 从磁盘加载了 %d 条交易记录', _allTradeRecords.length);
    }
  } catch (_) {}
}

class TokenMonitor extends EventEmitter {
  constructor() {
    super();
    this._tokens    = new Map();
    this._pollTimer = null;
    this._started   = false;
    // 止损锁：防止同一 token 并发触发多次止损
    this._stopLossLocks = new Set();
    this._slPollTimer = null;  // 独立止损轮询
    this._persistTimer = null; // ★ V5: 定时持久化
  }

  start() {
    if (this._started) return;
    this._started = true;

    dataStore.init();
    _loadPersistedTrades();
    dataStore.startFlush();

    birdeye.priceStream.start();
    heliusWs.start();

    this._scheduleNextPoll();
    this._startStopLossPoller();  // ★ 500ms 独立止损轮询
    logger.info('[Monitor] 启动 | 轮询=%ds K线=%ds 止损轮询=%ds 冷却=%ds DRY_RUN=%s',
      POLL_SEC, KLINE_SEC, SL_POLL_SEC, SELL_COOLDOWN_SEC, DRY_RUN);
    logger.info('[Monitor]   BirdeyeWS=%s  HeliusWS=%s',
      birdeye.priceStream.isConnected() ? '已连接' : '连接中',
      heliusWs.isConnected() ? '已连接' : '连接中');
    logger.info('[Monitor]   移动止损=%s  激活线=+%s%%  回撤线=%s%%',
      TRAILING_STOP_ENABLED ? '开启' : '关闭', TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT);

    // ★ 加载持久化的代币列表（延迟500ms，等 WS 连接建立）
    setTimeout(() => this._loadPersistedTokens(), 500);

    // ★ V5: 定时持久化状态（每60秒），确保崩溃/重启后不丢失RSI预热和持仓
    this._persistTimer = setInterval(() => this._persistTokens(), 60000);

    // ★ V5: FDV/LP/Age 巡检（每 OVERVIEW_PATROL_SEC 秒一轮，分散请求）
    this._patrolTimer = null;
    this._startOverviewPatrol();
    logger.info('[Monitor]   FDV退出<$%d  LP退出<$%d  最大监控=%d  巡检=%ds',
      FDV_EXIT, LP_EXIT, MAX_TOKENS, OVERVIEW_PATROL_SEC);
  }

  _loadPersistedTokens() {
    try {
      const tokens = dataStore.loadTokens();
      if (!tokens || tokens.length === 0) return;
      logger.info('[Monitor] 从磁盘恢复 %d 个监控代币...', tokens.length);
      for (const t of tokens) {
        if (t.address && t.symbol) {
          const added = this.addToken(t.address, t.symbol, t.meta || {});
          if (!added) continue;

          // ★ V5: 恢复保存的运行状态
          const state = this._tokens.get(t.address);
          if (!state) continue;

          // 恢复 FDV/LP/Age
          if (t.fdv != null) state.fdv = t.fdv;
          if (t.lp != null) state.lp = t.lp;
          if (t.createdAt != null) state.createdAt = t.createdAt;

          // ★ 不恢复 RSI 缓存（_rsiAvgGain 等）— 从 ticks 重新计算
          //   旧缓存的 lastClose 跟当前价格可能差很远，stepRSI 会算出虚高RSI

          // 恢复持仓状态
          if (t.inPosition && t.position) {
            state.inPosition = true;
            state.position   = t.position;
            state.tradeCount = t.tradeCount || 0;
            logger.info('[Monitor] ♻️ 恢复 %s 持仓状态: entry=%.6f SOL=%.4f',
              t.symbol, t.position.entryPriceUsd, t.position.solIn);
          } else {
            state.tradeCount = t.tradeCount || 0;
          }

          // 恢复冷却期
          if (t._sellCooldownUntil && t._sellCooldownUntil > Date.now()) {
            state._sellCooldownUntil = t._sellCooldownUntil;
          }

          // ★ V5: 从磁盘加载历史 ticks 恢复 K 线数据
          try {
            const savedTicks = dataStore.loadTicks(t.address);
            if (savedTicks && savedTicks.length > 0) {
              // 加载最近2小时的 ticks（5分钟K线 × RSI(7) 需要至少9根 = 45分钟，留余量）
              const cutoff = Date.now() - 2 * 60 * 60 * 1000;
              const recentTicks = savedTicks.filter(tk => tk.ts > cutoff);
              if (recentTicks.length > 0) {
                state.ticks = recentTicks;
                logger.info('[Monitor] ♻️ 恢复 %s %d 条历史tick（最近2小时）',
                  t.symbol, recentTicks.length);
              }
            }
          } catch (_) {}

          // ★ 重启后重新拉取历史K线（historicalCandles 不持久化，重启必须重拉）
          birdeye.getOHLCV(t.address, KLINE_SEC, HIST_BARS).then(histCandles => {
            const s = this._tokens.get(t.address);
            if (!s || !histCandles || histCandles.length === 0) return;
            s.historicalCandles = histCandles;
            logger.info('[Monitor] ♻️ %s 历史K线重载: %d 根', t.symbol, histCandles.length);
          }).catch(() => {});
        }
      }
    } catch (err) {
      logger.error('[Monitor] 加载持久化代币失败: %s', err.message);
    }
  }

  _persistTokens() {
    try {
      const list = Array.from(this._tokens.values()).map(s => ({
        address: s.address,
        symbol:  s.symbol,
        meta:    s.meta || {},
        // ★ V5: 保存运行状态，重启后不丢失
        fdv:            s.fdv,
        lp:             s.lp,
        createdAt:      s.createdAt,
        inPosition:     s.inPosition,
        position:       s.position,
        tradeCount:     s.tradeCount,
        _sellCooldownUntil: s._sellCooldownUntil,
        // ★ 不再保存 RSI 缓存状态（_rsiAvgGain 等）
        //   恢复后由 ticks 重新聚合 K 线重算，避免旧缓存与新 ticks 不匹配导致 RSI 虚高
      }));
      dataStore.saveTokens(list);
    } catch (_) {}
  }

  stop() {
    this._started = false;
    if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    if (this._slPollTimer) { clearInterval(this._slPollTimer); this._slPollTimer = null; }
    if (this._persistTimer) { clearInterval(this._persistTimer); this._persistTimer = null; }
    if (this._patrolTimer) { clearTimeout(this._patrolTimer); this._patrolTimer = null; }
    this._persistTokens();  // ★ V5: 关闭前最后保存一次
    birdeye.priceStream.stop();
    heliusWs.stop();
    dataStore.stopFlush();
  }

  addToken(address, symbol, meta = {}) {
    if (this._tokens.has(address)) {
      logger.warn('[Monitor] %s 已在监控中，忽略', symbol);
      return false;
    }

    // ★ V5: 最大监控数检查
    if (this._tokens.size >= MAX_TOKENS) {
      const evicted = this._evictForNewToken();
      if (!evicted) {
        logger.warn('[Monitor] %s 无法添加：监控已满(%d/%d)', symbol, this._tokens.size, MAX_TOKENS);
        return false;
      }
    }

    const now = Date.now();
    const state = {
      address,
      symbol,
      meta,
      fdv               : meta.fdv ?? null,
      lp                : meta.lp  ?? null,
      createdAt         : meta.createdAt ?? null,  // ★ V5: 代币创建时间(ms)
      addedAt           : now,
      ticks             : [],
      historicalCandles : [],  // ★ 启动时从 Birdeye 拉取的历史K线（用于EMA99/RSI预热）
      inPosition        : false,
      position          : null,
      tradeCount        : 0,       // 完成的买卖轮次数
      tradeLogs         : [],
      tradeRecords      : [],
      _prevRsiRealtime  : NaN,
      _prevRsiTs        : 0,
      _lastBuyCandle    : -1,
      _lastSellCandle   : -1,
      _lastPanicSellTs  : 0,       // RSI_PANIC 时间防抖（毫秒时间戳）
      _lastPriceUsd     : null,
      _lastPriceTs      : 0,
      // ★ 实时 RSI 下穿检测缓存（每个 WS tick 更新，不依赖 1s 轮询）
      _rsiAvgGain       : NaN,     // 最新已收盘K线的 avgGain
      _rsiAvgLoss       : NaN,     // 最新已收盘K线的 avgLoss
      _rsiLastClose     : NaN,     // 最新已收盘K线的 close
      _rsiLastCandleTs  : -1,      // 对应的 K 线 openTime（用于检测 K 线是否刷新）
      _rsiPrevTickRsi   : NaN,     // 保留字段（暂未使用）
      _slPollPrevRsi    : NaN,     // 500ms轮询的上一次实时RSI（用于下穿检测）
      _wsTickPrevRsi    : NaN,     // ★ WS tick的上一次实时RSI（用于下穿检测）
      _lastRsiCrossSellTs: 0,      // ★ RSI下穿70的时间防抖（毫秒时间戳）
      // ★ 多次买卖相关
      _sellCooldownUntil: 0,       // 卖出后冷却到期时间戳
      _selling          : false,   // 正在执行卖出中（防并发）
    };

    this._tokens.set(address, state);

    birdeye.priceStream.subscribe(address, (price, ts, ohlcv) => {
      this._onBirdeyePrice(address, price, ts);
    });

    heliusWs.subscribe(address, symbol, (trade) => {
      this._onChainTrade(address, trade);
    });

    // ★ 异步拉取 overview（Age/FDV/LP）+ 历史K线（EMA99/RSI预热）
    (async () => {
      const s = this._tokens.get(address);
      if (!s) return;

      // 1. 拉取 overview
      try {
        const ov = await birdeye.getOverview(address);
        if (ov) {
          if (ov.createdAt) s.createdAt = ov.createdAt;
          if (ov.fdv !== null && Number.isFinite(ov.fdv)) s.fdv = ov.fdv;
          if (ov.liquidity !== null && Number.isFinite(ov.liquidity)) s.lp = ov.liquidity;
          logger.debug('[Monitor] %s overview初始化: fdv=$%s age=%s',
            symbol,
            s.fdv ? Math.round(s.fdv) : '?',
            s.createdAt ? Math.round((Date.now() - s.createdAt) / 3600000) + 'h' : '?');
        }
      } catch (_) {}

      // 2. 拉取历史K线（用于 EMA99/RSI 预热，无需等待K线自然积累）
      try {
        const histCandles = await birdeye.getOHLCV(address, KLINE_SEC, HIST_BARS);
        if (histCandles && histCandles.length > 0) {
          s.historicalCandles = histCandles;
          logger.info('[Monitor] %s 历史K线预热: %d 根 (EMA99/RSI立即可用)',
            symbol, histCandles.length);
        }
      } catch (_) {}
    })();

    logger.info("[Monitor] ➕ 开始监控 %s (%s) | DRY_RUN=%s",
      symbol, address, DRY_RUN);
    this._broadcastTokenList();
    this._persistTokens();  // ★ 保存到磁盘
    return true;
  }

  async removeToken(address, reason = 'manual') {
    const state = this._tokens.get(address);
    if (!state) return;

    logger.info('[Monitor] ➖ 移除 %s，原因: %s (共完成%d笔交易)', state.symbol, reason, state.tradeCount);

    // 到期/手动移除时如仍持仓，强制卖出
    if (state.inPosition && !state._selling) {
      logger.info('[Monitor] 📤 持仓中，先执行卖出...');
      await this._doSell(state, `FORCED_EXIT(${reason})`);
    }

    dataStore.flushTicks();

    birdeye.priceStream.unsubscribe(address);
    heliusWs.unsubscribe(address);

    this._tokens.delete(address);
    this._stopLossLocks.delete(address);
    birdeye.clearCache(address);
    this._broadcastTokenList();
    this._persistTokens();  // ★ 保存到磁盘
  }

  getTokens() {
    return Array.from(this._tokens.values()).map(s => this._stateSnapshot(s));
  }

  getToken(address) {
    const s = this._tokens.get(address);
    return s ? this._stateSnapshot(s) : null;
  }

  // ── Birdeye WS 实时价格回调（<150ms 延迟） ─────────────────────

  _onBirdeyePrice(address, price, ts) {
    const state = this._tokens.get(address);
    if (!state) return;

    state._lastPriceUsd = price;
    state._lastPriceTs  = ts;

    const tick = { price, ts, source: 'price' };
    state.ticks.push(tick);

    dataStore.appendTick(address, {
      price, ts, source: 'price', symbol: state.symbol,
    });

    // ★ 快速止损检查（持仓中 + 未在卖出中）
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 快速止损触发 %s @ %.8f | %s | 第%d笔',
          state.symbol, price, sl.reason, state.tradeCount + 1);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 快速止损执行失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
        return; // 已触发卖出，不再检查 RSI
      }

      // ★★ 实时 RSI 卖出检查（每个 WS tick 都算，不等 K 线收盘）
      this._checkRealtimeRsiSell(state, price);
    }
  }

  /**
   * ★ V5 修复: 实时 RSI 卖出检查
   *   - RSI恐慌卖(>80): 只信任已收盘K线RSI，不用stepRSI（避免K线内波动导致虚假高RSI）
   *   - RSI下穿70: 仍用stepRSI实时检测（下穿检测对精度要求低于绝对值判断）
   */
  _checkRealtimeRsiSell(state, price) {
    const avgGain   = state._rsiAvgGain;
    const avgLoss   = state._rsiAvgLoss;
    const lastClose = state._rsiLastClose;
    if (!Number.isFinite(avgGain) || !Number.isFinite(avgLoss) || !Number.isFinite(lastClose)) return;

    // 用当前实时价格计算实时 RSI
    const rsiNow = stepRSI(avgGain, avgLoss, lastClose, price);
    if (!Number.isFinite(rsiNow)) return;

    const prevRsi = state._wsTickPrevRsi;
    const now = Date.now();

    // 更新上一次的实时 RSI（用于下穿检测）
    state._wsTickPrevRsi = rsiNow;

    if (!Number.isFinite(prevRsi)) return; // 第一个 tick，没有 prev，跳过

    // ── RSI > 80 恐慌卖 — ★ V5 改为只在主轮询的已收盘K线RSI中触发 ──
    //    stepRSI 在K线内波动剧烈时容易算出虚假高值（如95），
    //    而已收盘K线RSI更稳定、与交易所显示一致。
    //    此处不再处理 RSI_PANIC，由 evaluateSignal 和 _stopLossPoll 负责。

    // ── RSI 下穿 70（实时：prevRsi >= 70 且 rsiNow < 70）──
    //    下穿检测只看方向变化，对绝对值精度要求低，stepRSI可信
    if (prevRsi >= _RSI_SELL && rsiNow < _RSI_SELL) {
      const lastCrossTs = state._lastRsiCrossSellTs ?? 0;
      if (now - lastCrossTs >= 2000) {
        state._lastRsiCrossSellTs = now;
        logger.info('[Monitor] ⚡ WS实时RSI下穿卖出 %s @ %.8f | RSI %.1f→%.1f',
          state.symbol, price, prevRsi, rsiNow);
        this._doSell(state, `RSI_CROSS_DOWN_70_RT(${prevRsi.toFixed(1)}→${rsiNow.toFixed(1)})`).catch(err => {
          logger.error('[Monitor] WS RSI下穿卖出失败 %s: %s', state.symbol, err.message);
        });
      }
    }
  }

  // ── Helius 链上交易回调 ──────────────────────────────────────

  _onChainTrade(address, trade) {
    const state = this._tokens.get(address);
    if (!state) return;

    const now = Date.now();
    const tick = {
      price:     trade.priceSol,
      ts:        trade.ts || now,
      solAmount: trade.solAmount,
      isBuy:     trade.isBuy,
      source:    'chain',
    };

    state.ticks.push(tick);

    dataStore.appendTick(address, {
      ...tick,
      symbol:    state.symbol,
      signature: trade.signature,
      owner:     trade.owner,
    });

    // ★ 链上交易也触发止损检查（用链上价格 × SOL/USD 估算）
    // 链上交易比 Birdeye WS 更快到达，不浪费这个信号
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      // 用最新的 Birdeye USD 价格做止损判断（链上 priceSol 单位不同，不能直接比）
      // 但如果有卖出交易且价格大幅下跌，说明市场在抛售
      const lastUsd = state._lastPriceUsd;
      if (lastUsd && trade.isBuy === false && trade.solAmount > 5) {
        // 大额卖出交易 → 触发紧急价格刷新
        this._urgentStopCheck(address, state);
      }
    }

    logger.debug('[HeliusTrade] %s %s %.4f SOL @ %.10f (%s)',
      state.symbol,
      trade.isBuy ? 'BUY' : 'SELL',
      trade.solAmount,
      trade.priceSol,
      trade.signature?.slice(0, 12) || '?');
  }

  // ── 紧急止损价格刷新（链上检测到大额卖出时触发）────────────
  async _urgentStopCheck(address, state) {
    if (state._selling || this._stopLossLocks.has(address)) return;
    try {
      // 绕过缓存直接拉最新价格
      const price = await birdeye.getPrice(address);
      if (!price || price <= 0) return;
      state._lastPriceUsd = price;
      state._lastPriceTs = Date.now();

      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 链上大卖触发止损 %s @ %.8f | %s', state.symbol, price, sl.reason);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 紧急止损失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
      }
    } catch (_) {}
  }

  // ── 独立止损轮询（每 500ms，不依赖 WS 推送） ─────────────────

  _startStopLossPoller() {
    if (this._slPollTimer) return;
    this._slPollTimer = setInterval(() => this._stopLossPoll(), SL_POLL_SEC * 1000);
  }

  async _stopLossPoll() {
    for (const [address, state] of this._tokens.entries()) {
      if (!state.inPosition || state._selling || this._stopLossLocks.has(address)) continue;

      try {
        // ★ V5: 优先用 WS 缓存价格（10秒内有效），避免对所有持仓币发 HTTP
        //   只有 WS 价格过期超过60秒才发 HTTP 兜底
        let price = birdeye.priceStream.getCachedPrice(address);
        if (price === null) {
          // WS 缓存失效，检查 state 里最近的价格是否够新（60秒内）
          if (state._lastPriceUsd && Date.now() - state._lastPriceTs < 60000) {
            price = state._lastPriceUsd;
          } else {
            price = await birdeye.getPrice(address);
          }
        }
        if (!price || price <= 0) continue;

        state._lastPriceUsd = price;
        state._lastPriceTs = Date.now();

        // ── 1. 止损/移动止损检查 ──────────────────────────────
        const sl = checkStopLoss(price, state);
        if (sl.shouldExit) {
          const holdSec = state.position?.buyTime ? Math.round((Date.now() - state.position.buyTime) / 1000) : 0;
          logger.info('[Monitor] ⚡ 止损轮询触发 %s @ %.8f | %s | 持仓%ds',
            state.symbol, price, sl.reason, holdSec);
          this._stopLossLocks.add(address);
          this._doSell(state, sl.reason).catch(err => {
            logger.error('[Monitor] 止损执行失败 %s: %s', state.symbol, err.message);
          }).finally(() => {
            this._stopLossLocks.delete(address);
          });
          continue;
        }

        // ── 2. RSI 卖出检查（双重方式：已收盘K线 + stepRSI实时估算） ──
        if (state.ticks.length > 0) {
          const { closed: rawCandles } = buildCandles(state.ticks, KLINE_SEC);
          const liveCandles = filterValidCandles(rawCandles); // RSI用
          // ★ 合并历史K线（RSI用）
          let closedCandles = liveCandles;
          if (state.historicalCandles && state.historicalCandles.length > 0) {
            const liveStart2 = liveCandles.length > 0 ? liveCandles[0].openTime : Infinity;
            const histFiltered2 = state.historicalCandles.filter(c => c.openTime < liveStart2);
            closedCandles = [...histFiltered2, ...liveCandles];
          }
          if (closedCandles.length >= RSI_CONFIG.RSI_PERIOD + 2) {
            const closes = closedCandles.map(c => c.close);
            const { rsiArray, avgGain, avgLoss } = calcRSIWithState(closes);
            const len     = closes.length;

            // ★ 同时缓存 avgGain/avgLoss/lastClose，供 WS tick 实时 RSI 使用
            const lastCandleTsPoll = closedCandles[len - 1].openTime;
            if (lastCandleTsPoll !== state._rsiLastCandleTs) {
              state._rsiAvgGain     = avgGain;
              state._rsiAvgLoss     = avgLoss;
              state._rsiLastClose   = closes[len - 1];
              state._rsiLastCandleTs = lastCandleTsPoll;
            }

            // ★ 用 stepRSI 计算实时 RSI（基于当前价格，而非等K线收盘）
            const rsiRealtime = stepRSI(avgGain, avgLoss, closes[len - 1], price);
            const rsiClosedLast = rsiArray[len - 1];  // 最新已收盘K线RSI（作为 prev 参考）

            // 取上一次轮询的实时 RSI 作为 prevRsi
            const prevRsiPoll = state._slPollPrevRsi;
            state._slPollPrevRsi = rsiRealtime;  // 保存本次，供下次比较

            if (Number.isFinite(rsiRealtime)) {
              // ★ V5: RSI > 80 恐慌卖 — 改为用已收盘K线RSI判断，不用stepRSI
              //   stepRSI在K线内波动时容易算出虚假高值
              if (Number.isFinite(rsiClosedLast) && rsiClosedLast > _RSI_PANIC) {
                const lastPanicTs = state._lastPanicSellTs ?? 0;
                if (Date.now() - lastPanicTs >= 2000) {
                  state._lastPanicSellTs = Date.now();
                  logger.info('[Monitor] ⚡ RSI恐慌卖出(K线) %s @ %.8f | RSI_K=%.1f>%d',
                    state.symbol, price, rsiClosedLast, _RSI_PANIC);
                  this._doSell(state, `RSI_PANIC(K=${rsiClosedLast.toFixed(1)}>${_RSI_PANIC})`).catch(err => {
                    logger.error('[Monitor] RSI恐慌卖出失败 %s: %s', state.symbol, err.message);
                  });
                }
              }
              // RSI 下穿70：支持两种 prev 来源
              //   a) 上次轮询的实时 RSI (prevRsiPoll) — 500ms 间隔的 tick-to-tick 比较
              //   b) 最新已收盘K线 RSI (rsiClosedLast) — K线级别的下穿
              else if (Number.isFinite(prevRsiPoll) && prevRsiPoll >= _RSI_SELL && rsiRealtime < _RSI_SELL) {
                const lastCrossTs = state._lastRsiCrossSellTs ?? 0;
                if (Date.now() - lastCrossTs >= 2000) {
                  state._lastRsiCrossSellTs = Date.now();
                  logger.info('[Monitor] ⚡ RSI下穿卖出(轮询RT) %s @ %.8f | RSI %.1f→%.1f',
                    state.symbol, price, prevRsiPoll, rsiRealtime);
                  this._doSell(state, `RSI_CROSS_DOWN_70(RT:${prevRsiPoll.toFixed(1)}→${rsiRealtime.toFixed(1)})`).catch(err => {
                    logger.error('[Monitor] RSI下穿卖出失败 %s: %s', state.symbol, err.message);
                  });
                }
              }
              // 备用：已收盘K线级别下穿（保留原逻辑作为兜底）
              else if (Number.isFinite(rsiClosedLast)) {
                const rsiPrevClosed = rsiArray[len - 2];
                if (Number.isFinite(rsiPrevClosed) && rsiPrevClosed >= _RSI_SELL && rsiClosedLast < _RSI_SELL) {
                  const candleTs = closedCandles[len - 1].openTime;
                  if (candleTs !== state._lastSellCandle) {
                    state._lastSellCandle = candleTs;
                    logger.info('[Monitor] ⚡ RSI下穿卖出(K线) %s @ %.8f | RSI %.1f→%.1f',
                      state.symbol, price, rsiPrevClosed, rsiClosedLast);
                    this._doSell(state, `RSI_CROSS_DOWN_70(K:${rsiPrevClosed.toFixed(1)}→${rsiClosedLast.toFixed(1)})`).catch(err => {
                      logger.error('[Monitor] RSI下穿卖出失败 %s: %s', state.symbol, err.message);
                    });
                  }
                }
              }
            }
          }
        }
      } catch (_) {}
    }
  }

  // ── 主轮询 ────────────────────────────────────────────────────

  _scheduleNextPoll() {
    if (!this._started) return;
    this._pollTimer = setTimeout(() => this._poll(), POLL_SEC * 1000);
  }

  async _poll() {
    const now = Date.now();
    const addresses = Array.from(this._tokens.keys());

    // ★ V5: 并发控制 — 最多10个同时执行，避免47+币同时发HTTP请求
    const CONCURRENCY = 10;
    for (let i = 0; i < addresses.length; i += CONCURRENCY) {
      const batch = addresses.slice(i, i + CONCURRENCY);
      await Promise.allSettled(batch.map(addr => this._pollOne(addr, now)));
    }
    this._scheduleNextPoll();
  }

  async _pollOne(address, now) {
    const state = this._tokens.get(address);
    if (!state) return;

    // 正在卖出中，跳过此轮
    if (state._selling) return;

    // 1. 获取价格
    // ★ 优先用 BirdeyeWS 已推送的最新价格（state._lastPriceUsd 由 _onBirdeyePrice 实时更新）
    //   只有 WS 价格超过 PRICE_STALE_MS 没更新，才发 HTTP 兜底请求
    //   这样避免每秒对48个币发HTTP，尤其是低流动性币WS长时间不推送的情况
    const PRICE_STALE_MS = parseInt(process.env.PRICE_STALE_MS || '60000', 10); // ★ V5: 默认60秒
    let price;
    const wsAge = state._lastPriceUsd ? now - state._lastPriceTs : Infinity;
    if (state._lastPriceUsd && wsAge < PRICE_STALE_MS) {
      // WS 价格足够新鲜，直接用，不发 HTTP
      price = state._lastPriceUsd;
    } else {
      // WS 价格过期或没有，发 HTTP 兜底
      try {
        price = await birdeye.getPrice(address);
        if (price && price > 0) {
          state._lastPriceUsd = price;
          state._lastPriceTs  = now;
        }
      } catch (err) {
        logger.warn('[Monitor] %s 价格拉取失败: %s', state.symbol, err.message);
        // 如果有旧价格，宁可用旧的继续跑 RSI，不要直接 return
        if (!state._lastPriceUsd) return;
        price = state._lastPriceUsd;
      }
    }
    if (!price || price <= 0) return;

    // 2. WS 不可用时补 tick（仅在 HTTP 兜底拉到新价格时才需要，WS 正常时由 _onBirdeyePrice 负责）
    if (wsAge >= PRICE_STALE_MS) {
      const tick = { price, ts: now, source: 'price' };
      state.ticks.push(tick);
      dataStore.appendTick(address, { price, ts: now, source: 'price', symbol: state.symbol });
    }

    // 4. FDV/LP 检查（只用缓存值，巡检会定期刷新）
    const fdv = birdeye.getCachedFdv(address);
    if (fdv !== null && Number.isFinite(fdv)) {
      state.fdv = fdv;  // 更新state
      if (fdv < FDV_EXIT) {
        logger.warn('[Monitor] %s FDV=$%d < $%d，退出', state.symbol, Math.round(fdv), FDV_EXIT);
        await this.removeToken(address, `FDV_TOO_LOW($${Math.round(fdv)})`);
        return;
      }
    }
    // LP 退出检查（用 state 中巡检更新的值）
    if (state.lp !== null && Number.isFinite(state.lp) && state.lp < LP_EXIT) {
      logger.warn('[Monitor] %s LP=$%d < $%d，退出', state.symbol, Math.round(state.lp), LP_EXIT);
      await this.removeToken(address, `LP_TOO_LOW($${Math.round(state.lp)})`);
      return;
    }

    // 5. 裁剪 ticks（保留最近 2 小时）
    // ★ V5: 用 findIndex+splice 替代 while+shift，O(1) vs O(n)
    const cutoff = now - 2 * 60 * 60 * 1000;
    if (state.ticks.length > 0 && state.ticks[0].ts < cutoff) {
      const idx = state.ticks.findIndex(t => t.ts >= cutoff);
      if (idx > 0) state.ticks.splice(0, idx);
      else if (idx === -1) state.ticks.length = 0;  // 全部过期
    }

    // 6. 聚合 K 线（历史K线 + 实时ticks合并）
    const { closed: rawClosedCandles, current: currentCandle } = buildCandles(state.ticks, KLINE_SEC);
    const liveClosed = filterValidCandles(rawClosedCandles); // RSI用：只含真实价格K线
    // ★ 合并历史K线（RSI/EMA用）：历史candles在前，实时candles在后
    let closedCandles = liveClosed;
    if (state.historicalCandles && state.historicalCandles.length > 0) {
      const liveStart = liveClosed.length > 0 ? liveClosed[0].openTime : Infinity;
      const histFiltered = state.historicalCandles.filter(c => c.openTime < liveStart);
      closedCandles = [...histFiltered, ...liveClosed];
    }
    // ★ 量能用：原始K线（含无价格的链上K线）+ 当前未收盘K线，历史K线在前
    // currentCandle 包含当前5分钟窗口内最新的链上交易，必须纳入量能统计
    const rawLiveAll = currentCandle
      ? [...rawClosedCandles, currentCandle]
      : rawClosedCandles;
    let rawForVolume = rawLiveAll;
    if (state.historicalCandles && state.historicalCandles.length > 0) {
      const liveStart = rawLiveAll.length > 0 ? rawLiveAll[0].openTime : Infinity;
      const histFiltered = state.historicalCandles.filter(c => c.openTime < liveStart);
      rawForVolume = [...histFiltered, ...rawLiveAll];
    }

    // 7. RSI + 量能信号评估
    const realtimePrice = currentCandle?.close ?? price;

    // ★ 诊断日志：打印量能数据来源（每个币每60秒一次）
    if (!state._lastVolLog || Date.now() - state._lastVolLog > 60000) {
      state._lastVolLog = Date.now();
      const chainTicks = state.ticks.filter(t => t.source === 'chain');
      const rawChainBuys  = rawForVolume.filter(c => !c.fromHistory).reduce((s,c)=>s+(c.buyVolume||0),0);
      const rawChainSells = rawForVolume.filter(c => !c.fromHistory).reduce((s,c)=>s+(c.sellVolume||0),0);
      logger.info('[VolDiag] %s | chainTicks=%d | rawCandles=%d(live=%d,hist=%d) | buyVol=%.4f sellVol=%.4f | currentCandle=%s',
        state.symbol,
        chainTicks.length,
        rawForVolume.length,
        rawForVolume.filter(c=>!c.fromHistory).length,
        rawForVolume.filter(c=>c.fromHistory).length,
        rawChainBuys, rawChainSells,
        currentCandle ? `open=${new Date(currentCandle.openTime).toISOString().slice(11,19)} buy=${(currentCandle.buyVolume||0).toFixed(4)} sell=${(currentCandle.sellVolume||0).toFixed(4)}` : 'null'
      );
    }

    const { rsi, prevRsi, signal, reason, volume, candleTs: signalCandleTs } = evaluateSignal(closedCandles, realtimePrice, state, rawForVolume);

    // 8. 记录信号
    if (reason && reason !== '' && reason !== 'rsi_rebase') {
      dataStore.appendSignal({
        ts: now, address, symbol: state.symbol,
        price, rsi: Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi: Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal, reason, volume, inPosition: state.inPosition,
        tradeCount: state.tradeCount,
      });
    }

    // 9. 广播实时数据
    wsHub.broadcast({
      type:        'tick',
      address,
      symbol:      state.symbol,
      price,
      fdv,
      lp:          state.lp,
      createdAt:   state.createdAt,
      rsi:         Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
      prevRsi:     Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
      signal,
      reason,
      closedCount: closedCandles.length,
      inPosition:  state.inPosition,
      volume,
      tradeCount:  state.tradeCount,
      cooldown:    state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
      dryRun:      DRY_RUN,
      ts:          now,
      birdeyeWs:   birdeye.priceStream.isConnected(),
      heliusWs:    heliusWs.isConnected(),
      heliusStats: heliusWs.getStats(),
    });

    logger.debug('[RSI] %s price=%.6f rsi=%.2f prev=%.2f signal=%s reason=%s trades=%d inPos=%s cool=%ds',
      state.symbol, price, rsi, prevRsi, signal || 'none', reason,
      state.tradeCount, state.inPosition,
      state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0);

    // 10. 执行信号
    if (signal === 'BUY' && !state.inPosition && this._canBuy(state, now)) {
      // ★ 冷却通过后才标记 _lastBuyCandle，防止冷却期内白白消耗K线槽位
      {
        const lastCandle = signalCandleTs ?? (closedCandles && closedCandles.length > 0
          ? closedCandles[closedCandles.length - 1].openTime : -1);
        state._lastBuyCandle = lastCandle;
      }
      // ★ 买入前强制刷新 FDV（绕过缓存，确保数据最新）
      const freshFdv = await birdeye.getFdvFresh(address);
      if (freshFdv !== null && Number.isFinite(freshFdv) && freshFdv < FDV_EXIT) {
        logger.warn('[Monitor] %s 买入被拒: FDV=$%d < $%d', state.symbol, Math.round(freshFdv), FDV_EXIT);
      } else {
        state.fdv = freshFdv ?? state.fdv;  // 更新最新 FDV
        await this._doBuy(state, price, reason);
      }
    } else if (signal === 'SELL' && state.inPosition && !state._selling) {
      await this._doSell(state, reason);
    }
  }

  // ── 是否可以买入 ────────────────────────────────────────────────

  _canBuy(state, now) {
    // 已在持仓中
    if (state.inPosition) return false;
    // 正在卖出中
    if (state._selling) return false;
    // 冷却期中
    if (now < state._sellCooldownUntil) {
      logger.debug('[Monitor] %s 冷却中，还剩 %ds',
        state.symbol, Math.ceil((state._sellCooldownUntil - now) / 1000));
      return false;
    }
    return true;
  }

  // ── 买入 ────────────────────────────────────────────────────────

  async _doBuy(state, price, reason) {
    const tradeNum = state.tradeCount + 1;
    logger.info('[Monitor] 🟢 BUY #%d %s @ %.8f | %s | DRY_RUN=%s',
      tradeNum, state.symbol, price, reason, DRY_RUN);
    state.inPosition = true;

    if (DRY_RUN) {
      const simulatedTokens = Math.floor(TRADE_SOL / price * 1e9);
      state.position = {
        entryPriceUsd : price,
        amountToken   : simulatedTokens,
        solIn         : TRADE_SOL,
        buyTxid       : `DRY_${Date.now()}`,
        buyTime       : Date.now(),
        buyReason     : reason,
        _peakPrice    : price,  // ★ 移动止损：初始峰值 = 买入价
      };
      state.tradeCount++;
      this._addTradeLog(state, { type: 'BUY', symbol: state.symbol, price, reason,
        txid: state.position.buyTxid, solIn: TRADE_SOL, dryRun: true, tradeNum });
      await this._createTradeRecord(state);
      logger.info('[Monitor] ✅ DRY_RUN BUY #%d %s @ %.8f  solIn=%.4f',
        tradeNum, state.symbol, price, TRADE_SOL);
    } else {
      try {
        const result = await trader.buy(state.address, state.symbol);

        // ★ 买单成交后，等 500ms 再查一次实际成交价
        //   避免用"信号触发时价格"做止损基准（memecoin 滑点可能很大）
        let actualEntryPrice = price;
        try {
          await new Promise(r => setTimeout(r, 500));
          const postFillPrice = await birdeye.getPrice(state.address);
          if (postFillPrice && postFillPrice > 0) {
            actualEntryPrice = postFillPrice;
            if (Math.abs(postFillPrice - price) / price > 0.02) {
              logger.warn('[Monitor] ⚠️ BUY #%d %s 成交价偏差: 信号=%.6f 实际=%.6f (%.1f%%)',
                tradeNum, state.symbol, price, postFillPrice,
                (postFillPrice - price) / price * 100);
            }
          }
        } catch (_) { /* 查询失败保留信号价 */ }

        state.position = {
          entryPriceUsd : actualEntryPrice,  // ★ 用实际成交后价格，不用信号触发时价格
          signalPriceUsd: price,             // 保留信号价用于参考
          amountToken   : result.amountOut,
          solIn         : result.solIn,
          buyTxid       : result.txid,
          buyTime       : Date.now(),
          buyReason     : reason,
          _peakPrice    : actualEntryPrice,  // ★ 移动止损：初始峰值 = 实际成交价
        };
        state.tradeCount++;
        this._addTradeLog(state, { type: 'BUY', symbol: state.symbol,
          price: actualEntryPrice, signalPrice: price, reason,
          txid: result.txid, solIn: result.solIn, tradeNum });
        await this._createTradeRecord(state);
        logger.info('[Monitor] ✅ BUY #%d %s  solIn=%.4f SOL  entryPrice=%.6f  txid=%s',
          tradeNum, state.symbol, result.solIn, actualEntryPrice, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ BUY #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        state.inPosition = false;
      }
    }
  }

  // ── 卖出（不再退出监控，重置状态等待下一轮） ────────────────────

  async _doSell(state, reason) {
    if (state._selling) return;  // 防并发
    state._selling = true;

    const isStopLoss = reason.includes('STOP_LOSS') || reason.includes('TAKE_PROFIT');
    const tradeNum = state.tradeCount;
    logger.info('[Monitor] 🔴 SELL #%d %s | %s | isStopLoss=%s | DRY_RUN=%s',
      tradeNum, state.symbol, reason, isStopLoss, DRY_RUN);

    if (DRY_RUN) {
      let currentPrice;
      try {
        currentPrice = await birdeye.getPrice(state.address);
      } catch (_) {
        currentPrice = state._lastPriceUsd
          || (state.ticks.length > 0 ? state.ticks[state.ticks.length - 1].price : 0)
          || state.position?.entryPriceUsd || 0;
      }

      const solIn  = state.position?.solIn ?? TRADE_SOL;
      const entryP = state.position?.entryPriceUsd ?? 0;
      const solOut = entryP > 0 ? solIn * (currentPrice / entryP) : 0;
      const pnlPct = entryP > 0 ? (currentPrice - entryP) / entryP * 100 : 0;
      const pnlSol = solOut - solIn;

      state.inPosition = false;
      this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
        txid: `DRY_${Date.now()}`, solOut, pnlSol, dryRun: true, tradeNum });
      this._finalizeTradeRecord(state, reason, solOut, pnlPct);

      logger.info('[Monitor] ✅ DRY_RUN SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)',
        tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct);
    } else {
      try {
        const result = await trader.sell(state.address, state.symbol, state.position, isStopLoss);
        const solOut  = result.solOut ?? 0;
        const solIn   = state.position?.solIn ?? TRADE_SOL;
        const pnlPct  = solIn > 0 ? (solOut - solIn) / solIn * 100 : 0;
        const pnlSol  = solOut - solIn;

        state.inPosition = false;
        this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
          txid: result.txid, solOut, pnlSol, elapsedMs: result.elapsedMs, tradeNum });
        this._finalizeTradeRecord(state, reason, solOut, pnlPct);

        logger.info('[Monitor] ✅ SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)  耗时=%dms  txid=%s',
          tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct, result.elapsedMs || 0, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ SELL #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        state.inPosition = false;
        this._finalizeTradeRecord(state, `SELL_FAILED(${reason})`, 0, -100);
      }
    }

    // ★ 重置状态，准备下一轮交易
    state._selling = false;
    state.position = null;

    // ★ 设置冷却期
    state._sellCooldownUntil = Date.now() + SELL_COOLDOWN_SEC * 1000;
    // 重置 RSI 穿越防抖（允许新的穿越信号）
    state._lastBuyCandle  = -1;
    state._lastSellCandle = -1;
    state._lastPanicSellTs = 0;
    state._lastRsiCrossSellTs = 0;  // ★ 重置实时RSI下穿防抖
    state._wsTickPrevRsi  = NaN;    // ★ 重置WS tick RSI历史
    state._slPollPrevRsi  = NaN;    // ★ 重置轮询RSI历史

    logger.info('[Monitor] 🔄 %s 第%d笔完成 | 冷却=%ds',
      state.symbol, tradeNum, SELL_COOLDOWN_SEC);
  }

  // ── 辅助工具 ────────────────────────────────────────────────────

  _addTradeLog(state, log) {
    state.tradeLogs.push({ ...log, ts: Date.now() });
    if (state.tradeLogs.length > 500) state.tradeLogs.shift();
    wsHub.broadcast({ type: 'trade_log', ...log, ts: Date.now() });
    this.emit('trade', log);
  }

  async _createTradeRecord(state) {
    if (!state.position) return;

    // ★ V5: 买入时优先用 FDV 缓存中的 LP（_fetchOverview 同时返回 fdv 和 lp）
    //   getFdvFresh 在 _doBuy 前已经调过了，缓存应该是热的
    let realTimeLp = state.lp;
    try {
      const cached = birdeye.getCachedFdv(state.address);
      // getCachedFdv只返回fdv，LP需要从overview缓存中取
      const lp = await birdeye.getLiquidity(state.address); // 会命中缓存
      if (lp !== null && Number.isFinite(lp)) {
        realTimeLp = lp;
        state.lp = lp;
      }
    } catch (_) {}

    const rec = {
      id:         `${state.address}_${state.tradeCount}_${Date.now()}`,
      address:    state.address,
      symbol:     state.symbol,
      tradeNum:   state.tradeCount,
      createdAt:  state.createdAt,  // ★ V5: 代币创建时间
      buyAt:      state.position.buyTime,
      buyTxid:    state.position.buyTxid,
      entryPrice: state.position.entryPriceUsd,
      entryFdv:   state.fdv,
      entryLp:    realTimeLp,
      solIn:      state.position.solIn,
      buyReason:  state.position.buyReason || '',
      dryRun:     DRY_RUN,
      exitAt:     null,
      exitReason: null,
      solOut:     null,
      pnlPct:    null,
      pnlSol:    null,
    };
    state.tradeRecords.push(rec);
    _allTradeRecords.unshift(rec);
    dataStore.appendTrade(rec);

    const cutoff = Date.now() - 24 * 3600 * 1000;
    while (_allTradeRecords.length && _allTradeRecords[_allTradeRecords.length - 1].buyAt < cutoff) {
      _allTradeRecords.pop();
    }
    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _finalizeTradeRecord(state, reason, solOut, pnlPct) {
    const rec = state.tradeRecords[state.tradeRecords.length - 1];
    if (!rec) return;
    rec.exitAt     = Date.now();
    rec.exitReason = reason;
    rec.solOut     = parseFloat(solOut.toFixed(6));
    rec.pnlPct    = parseFloat(pnlPct.toFixed(2));
    rec.pnlSol    = parseFloat((solOut - (state.position?.solIn ?? 0)).toFixed(6));

    dataStore.updateTrade(rec.id, {
      exitAt:     rec.exitAt,
      exitReason: rec.exitReason,
      solOut:     rec.solOut,
      pnlPct:    rec.pnlPct,
      pnlSol:    rec.pnlSol,
    });

    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _stateSnapshot(state) {
    const now = Date.now();
    return {
      address:      state.address,
      symbol:       state.symbol,
      addedAt:      state.addedAt,
      createdAt:    state.createdAt,
      inPosition:   state.inPosition,
      tradeCount:   state.tradeCount,
      cooldown:     state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
      tradeLogs:    state.tradeLogs,
      tradeRecords: state.tradeRecords,
      dryRun:       DRY_RUN,
      lastPrice:    state._lastPriceUsd,
      lastPriceTs:  state._lastPriceTs,
      fdv:          state.fdv,
      lp:           state.lp,
    };
  }

  _broadcastTokenList() {
    wsHub.broadcast({ type: 'token_list', tokens: this.getTokens() });
  }

  // ── ★ V5: FDV/LP/Age 巡检（分散请求，每轮间隔 OVERVIEW_PATROL_SEC）──────

  _startOverviewPatrol() {
    // 启动后延迟5秒开始第一轮巡检（尽快拿到Age/FDV/LP数据）
    this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), 5000);
  }

  async _runOverviewPatrol() {
    if (!this._started) return;
    const addresses = Array.from(this._tokens.keys());
    if (addresses.length === 0) {
      this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), OVERVIEW_PATROL_SEC * 1000);
      return;
    }

    // 分散请求：每个币之间间隔 2 秒，95个币约3分钟完成一轮
    const INTERVAL_PER_TOKEN = 2000;
    logger.info('[Patrol] 开始 FDV/LP/Age 巡检，%d 个代币，预计 %ds',
      addresses.length, Math.ceil(addresses.length * INTERVAL_PER_TOKEN / 1000));

    for (let i = 0; i < addresses.length; i++) {
      if (!this._started) return;
      const address = addresses[i];
      const state = this._tokens.get(address);
      if (!state) continue;

      try {
        // ★ createdAt 为空时强制绕过缓存重新拉取（确保Age数据能拿到）
        if (!state.createdAt) birdeye.clearCache(address);
        const overview = await birdeye.getOverview(address);
        if (!overview) continue;

        // 更新 state
        if (overview.fdv !== null && Number.isFinite(overview.fdv)) state.fdv = overview.fdv;
        if (overview.liquidity !== null && Number.isFinite(overview.liquidity)) state.lp = overview.liquidity;
        if (overview.createdAt) state.createdAt = overview.createdAt; // ★ 始终更新，确保Age数据存在

        // ★ FDV 退出检查
        if (state.fdv !== null && Number.isFinite(state.fdv) && state.fdv < FDV_EXIT) {
          logger.warn('[Patrol] %s FDV=$%d < $%d，退出监控', state.symbol, Math.round(state.fdv), FDV_EXIT);
          await this.removeToken(address, `FDV_TOO_LOW($${Math.round(state.fdv)})`);
          continue;
        }

        // ★ LP 退出检查
        if (state.lp !== null && Number.isFinite(state.lp) && state.lp < LP_EXIT) {
          logger.warn('[Patrol] %s LP=$%d < $%d，退出监控', state.symbol, Math.round(state.lp), LP_EXIT);
          await this.removeToken(address, `LP_TOO_LOW($${Math.round(state.lp)})`);
          continue;
        }

        logger.debug('[Patrol] %s FDV=$%s LP=$%s age=%s',
          state.symbol,
          state.fdv ? Math.round(state.fdv) : '?',
          state.lp ? Math.round(state.lp) : '?',
          state.createdAt ? Math.round((Date.now() - state.createdAt) / 3600000) + 'h' : '?');
      } catch (err) {
        logger.warn('[Patrol] %s 巡检失败: %s', state.symbol, err.message);
      }

      // 等待间隔再查下一个
      if (i < addresses.length - 1) {
        await new Promise(r => setTimeout(r, INTERVAL_PER_TOKEN));
      }
    }

    logger.info('[Patrol] 巡检完成，下次 %ds 后', OVERVIEW_PATROL_SEC);
    this._patrolTimer = setTimeout(() => this._runOverviewPatrol(), OVERVIEW_PATROL_SEC * 1000);
  }

  // ── ★ V6: 监控数满时清理（按24h链上交易量(SOL)排序，清理量最小的）──────

  _evict24hVolume(state) {
    // 统计 state.ticks 中过去24小时的链上交易量(SOL)
    const cutoff = Date.now() - 24 * 3600 * 1000;
    let vol = 0;
    for (const t of state.ticks) {
      if (t.source === 'chain' && t.ts >= cutoff && t.solAmount > 0) {
        vol += t.solAmount;
      }
    }
    return vol;
  }

  _evictForNewToken() {
    if (this._tokens.size < MAX_TOKENS) return true; // 有空位

    // 按24h链上交易量升序排，量最小的（最不活跃）优先被清理
    const candidates = Array.from(this._tokens.values())
      .filter(s => !s.inPosition && !s._selling)  // 不清理持仓中的
      .map(s => ({ state: s, vol24h: this._evict24hVolume(s) }))
      .sort((a, b) => a.vol24h - b.vol24h);  // 交易量最小的排前面

    if (candidates.length === 0) {
      logger.warn('[Monitor] 监控已满(%d/%d)且所有代币都持仓中，无法清理', this._tokens.size, MAX_TOKENS);
      return false;
    }

    const { state: victim, vol24h } = candidates[0];
    logger.info('[Monitor] 🧹 监控已满(%d/%d)，清理24h量最低代币 %s（%.2f SOL）',
      this._tokens.size, MAX_TOKENS, victim.symbol, vol24h);
    this.removeToken(victim.address, `EVICTED(vol24h=${vol24h.toFixed(2)}SOL)`);
    return true;
  }
}

function getAllTradeRecords() {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const memRecords = _allTradeRecords.filter(r => r.buyAt > cutoff);
  if (memRecords.length === 0) {
    return dataStore.loadTrades().filter(r => r.buyAt > cutoff);
  }
  return memRecords;
}

const monitor = new TokenMonitor();
module.exports = monitor;
module.exports.getAllTradeRecords = getAllTradeRecords;
module.exports.DRY_RUN = DRY_RUN;
