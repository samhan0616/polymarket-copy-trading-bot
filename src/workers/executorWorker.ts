import { parentPort, workerData } from 'worker_threads';
import { ClobClient } from '@polymarket/clob-client';
import createClobClient from '../utils/createClobClient';
import fetchData from '../utils/fetchData';
import getMyBalance from '../utils/getMyBalance';
import postOrder from '../utils/postOrder';
import Logger from '../utils/logger';
import { ENV } from '../config/env';
import { QueueActivity } from '../services/activityQueue';
import { UserActivityInterface, UserPositionInterface } from '../interfaces/User';

const PROXY_WALLET = ENV.PROXY_WALLET;
const TRADE_AGGREGATION_ENABLED = ENV.TRADE_AGGREGATION_ENABLED;
const TRADE_AGGREGATION_WINDOW_SECONDS = ENV.TRADE_AGGREGATION_WINDOW_SECONDS;
const TRADE_AGGREGATION_MIN_TOTAL_USD = 1.0;
const PAPER_TRADING_ENABLED = ENV.PAPER_TRADING_ENABLED;
const PAPER_TRADING_BALANCE_USD = ENV.PAPER_TRADING_BALANCE_USD;
const AGGREGATION_CHECK_INTERVAL_MS = 500;

interface AggregatedTrade {
    userAddress: string;
    conditionId: string;
    asset: string;
    side: string;
    slug?: string;
    eventSlug?: string;
    trades: QueueActivity[];
    totalUsdcSize: number;
    averagePrice: number;
    firstTradeTime: number;
    lastTradeTime: number;
}

const tradeAggregationBuffer: Map<string, AggregatedTrade> = new Map();

class PaperTrader {
    balance: number;
    positions: Map<string, { conditionId: string; asset: string; size: number; invested: number; avgPrice: number }>;

    constructor(initialUsd: number) {
        this.balance = initialUsd;
        this.positions = new Map();
    }

    getBalance(): number {
        return this.balance;
    }

    executeTrade(trade: QueueActivity | UserActivityInterface): boolean {
        const usdc = trade.usdcSize || 0;
        const size = (trade as any).size || 0;
        const price = (trade as any).price || 0;
        const cid = (trade as any).conditionId;

        if ((trade as any).side === 'BUY') {
            if (this.balance < usdc) {
                Logger.info(`Paper-trade: Insufficient balance for BUY $${usdc.toFixed(2)} — balance $${this.balance.toFixed(2)}`);
                return false;
            }
            this.balance -= usdc;
            const existing = this.positions.get(cid) || { conditionId: cid, asset: (trade as any).asset || '', size: 0, invested: 0, avgPrice: 0 };
            existing.size += size;
            existing.invested += usdc;
            existing.avgPrice = existing.size > 0 ? existing.invested / existing.size : price;
            this.positions.set(cid, existing);
        } else {
            const existing = this.positions.get(cid);
            if (!existing || existing.size < size) {
                Logger.info(`Paper-trade: Insufficient position for SELL ${size} units — held ${existing?.size || 0}`);
                return false;
            }
            this.balance += usdc;
            existing.size -= size;
            existing.invested -= usdc;
            if (existing.size === 0) this.positions.delete(cid);
            else this.positions.set(cid, existing);
        }

        Logger.info(`Paper-trade: ${(trade as any).side} $${usdc.toFixed(2)} — balance $${this.balance.toFixed(2)}`);
        return true;
    }

    getUserPortfolioValue(): number {
        let total = 0;
        for (const p of this.positions.values()) total += p.invested || 0;
        return total;
    }
}

const paperTrader = PAPER_TRADING_ENABLED ? new PaperTrader(PAPER_TRADING_BALANCE_USD) : null;

const getAggregationKey = (trade: QueueActivity): string => {
    return `${trade.userAddress}:${trade.conditionId}:${trade.asset}:${trade.side}`;
};

const addToAggregationBuffer = (trade: QueueActivity): void => {
    const key = getAggregationKey(trade);
    const existing = tradeAggregationBuffer.get(key);
    const now = Date.now();

    if (existing) {
        existing.trades.push(trade);
        existing.totalUsdcSize += trade.usdcSize || 0;
        const totalValue = existing.trades.reduce((sum, t) => sum + (t.usdcSize || 0) * (t.price || 0), 0);
        existing.averagePrice = existing.totalUsdcSize > 0 ? totalValue / existing.totalUsdcSize : trade.price || 0;
        existing.lastTradeTime = now;
    } else {
        tradeAggregationBuffer.set(key, {
            userAddress: trade.userAddress,
            conditionId: trade.conditionId,
            asset: trade.asset,
            side: trade.side || 'BUY',
            slug: trade.slug,
            eventSlug: trade.eventSlug,
            trades: [trade],
            totalUsdcSize: trade.usdcSize || 0,
            averagePrice: trade.price || 0,
            firstTradeTime: now,
            lastTradeTime: now,
        });
    }
};

const getReadyAggregatedTrades = (): AggregatedTrade[] => {
    const ready: AggregatedTrade[] = [];
    const now = Date.now();
    const windowMs = TRADE_AGGREGATION_WINDOW_SECONDS * 1000;

    for (const [key, agg] of tradeAggregationBuffer.entries()) {
        const timeElapsed = now - agg.firstTradeTime;

        if (timeElapsed >= windowMs) {
            if (agg.totalUsdcSize >= TRADE_AGGREGATION_MIN_TOTAL_USD) {
                ready.push(agg);
            } else {
                Logger.info(
                    `Trade aggregation for ${agg.userAddress} on ${agg.slug || agg.asset}: $${agg.totalUsdcSize.toFixed(2)} total from ${agg.trades.length} trades below minimum ($${TRADE_AGGREGATION_MIN_TOTAL_USD}) - dropping`
                );
            }
            tradeAggregationBuffer.delete(key);
        }
    }

    return ready;
};

const doTrading = async (clobClient: ClobClient, trades: QueueActivity[], workerLabel: string) => {
    for (const trade of trades) {
        const receivedAt = Date.now();
        const traderTimestampMs = trade.timestamp > 1e12 ? trade.timestamp : trade.timestamp * 1000;
        const detectedAt = (trade as any)._detectedAt || receivedAt;
        
        const totalLatency = receivedAt - traderTimestampMs;
        const queueLatency = receivedAt - detectedAt;
        
        Logger.info(`[${workerLabel}] ⏱️  LATENCY BREAKDOWN | TxHash: ${trade.transactionHash?.slice(0, 10)}...`);
        Logger.info(`[${workerLabel}]   Activity→Now: ${totalLatency}ms | Detection→Receipt: ${queueLatency}ms`);

        Logger.trade(trade.userAddress, trade.side || 'UNKNOWN', {
            asset: trade.asset,
            side: trade.side,
            amount: trade.usdcSize,
            price: trade.price,
            slug: trade.slug,
            eventSlug: trade.eventSlug,
            transactionHash: trade.transactionHash,
        });

        if (PAPER_TRADING_ENABLED && paperTrader) {
            const my_balance = paperTrader.getBalance();
            const user_balance = paperTrader.getUserPortfolioValue();
            Logger.balance(my_balance, user_balance, trade.userAddress);
            const executed = paperTrader.executeTrade(trade);
            if (!executed) {
                continue;
            }
        } else {
            const fetchStart = Date.now();
            const [my_positions, user_positions, my_balance] = await Promise.all([
                fetchData(`https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`),
                fetchData(`https://data-api.polymarket.com/positions?user=${trade.userAddress}`),
                getMyBalance(PROXY_WALLET)
            ]);
            const fetchLatency = Date.now() - fetchStart;
            Logger.info(`[${workerLabel}]   API fetch (positions + balance): ${fetchLatency}ms`);
            
            const my_position = my_positions.find(
                (position: UserPositionInterface) => position.conditionId === trade.conditionId
            );
            const user_position = user_positions.find(
                (position: UserPositionInterface) => position.conditionId === trade.conditionId
            );

            const user_balance = user_positions.reduce((total: number, pos: UserPositionInterface) => {
                return total + (pos.currentValue || 0);
            }, 0);

            Logger.balance(my_balance, user_balance, trade.userAddress);

            const orderStart = Date.now();
            await postOrder(
                clobClient,
                trade.side === 'BUY' ? 'buy' : 'sell',
                my_position,
                user_position,
                trade,
                my_balance,
                user_balance,
                trade.userAddress
            );
            const orderLatency = Date.now() - orderStart;
            const executionLatency = Date.now() - receivedAt;
            
            Logger.info(`[${workerLabel}]   Order placement: ${orderLatency}ms | Total execution: ${executionLatency}ms`);
            Logger.info(`[${workerLabel}]   🏁 TOTAL E2E: ${Date.now() - traderTimestampMs}ms (Activity→Order Complete)`);
        }

        Logger.separator();
    }
};

const doAggregatedTrading = async (clobClient: ClobClient, aggregatedTrades: AggregatedTrade[]) => {
    for (const agg of aggregatedTrades) {
        Logger.header(`📊 AGGREGATED TRADE (${agg.trades.length} trades combined)`);
        Logger.info(`Market: ${agg.slug || agg.asset}`);
        Logger.info(`Side: ${agg.side}`);
        Logger.info(`Total volume: $${agg.totalUsdcSize.toFixed(2)}`);
        Logger.info(`Average price: $${agg.averagePrice.toFixed(4)}`);

        const firstTrade = agg.trades[0];
        const traderTimestampMs = firstTrade.timestamp > 1e12 ? firstTrade.timestamp : firstTrade.timestamp * 1000;
        const latencyMs = Date.now() - traderTimestampMs;
        Logger.info(`Aggregated trade latency: ${latencyMs} ms (from first trader's trade to execution)`);

        const [my_positions, user_positions, my_balance] = await Promise.all([
            fetchData(`https://data-api.polymarket.com/positions?user=${PROXY_WALLET}`),
            fetchData(`https://data-api.polymarket.com/positions?user=${agg.userAddress}`),
            getMyBalance(PROXY_WALLET)
        ]);
        const my_position = my_positions.find(
            (position: UserPositionInterface) => position.conditionId === agg.conditionId
        );
        const user_position = user_positions.find(
            (position: UserPositionInterface) => position.conditionId === agg.conditionId
        );

        const user_balance = user_positions.reduce((total: number, pos: UserPositionInterface) => {
            return total + (pos.currentValue || 0);
        }, 0);

        Logger.balance(my_balance, user_balance, agg.userAddress);

        const syntheticTrade: UserActivityInterface = {
            ...agg.trades[0],
            usdcSize: agg.totalUsdcSize,
            price: agg.averagePrice,
            side: agg.side as 'BUY' | 'SELL',
        };

        await postOrder(
            clobClient,
            agg.side === 'BUY' ? 'buy' : 'sell',
            my_position,
            user_position,
            syntheticTrade,
            my_balance,
            user_balance,
            agg.userAddress
        );

        Logger.separator();
    }
};

let isRunning = true;
let aggregationTimer: NodeJS.Timeout | null = null;
let aggregationFlushInProgress = false;
const workerLabel: string = `executor-${workerData?.workerId || 0}`;

const startAggregationFlusher = (clobClient: ClobClient) => {
    if (!TRADE_AGGREGATION_ENABLED || aggregationTimer) return;

    aggregationTimer = setInterval(async () => {
        if (!isRunning || aggregationFlushInProgress) return;

        const readyAggregations = getReadyAggregatedTrades();
        if (readyAggregations.length === 0) return;

        aggregationFlushInProgress = true;
        try {
            Logger.header(
                `⚡ ${readyAggregations.length} AGGREGATED TRADE${readyAggregations.length > 1 ? 'S' : ''} READY`
            );
            await doAggregatedTrading(clobClient, readyAggregations);
        } catch (error) {
            Logger.error(`Failed to process aggregated trades: ${error}`);
        } finally {
            aggregationFlushInProgress = false;
        }
    }, AGGREGATION_CHECK_INTERVAL_MS);
};

const stopAggregationFlusher = () => {
    if (aggregationTimer) {
        clearInterval(aggregationTimer);
        aggregationTimer = null;
    }
};

const localQueue: QueueActivity[] = [];

const dequeueLoop = async (clobClient: ClobClient) => {
    while (isRunning) {
        const activity = localQueue.shift();
        if (!activity) {
            await new Promise((resolve) => setTimeout(resolve, 200));
            continue;
        }

        if (
            TRADE_AGGREGATION_ENABLED &&
            activity.side === 'BUY' &&
            (activity.usdcSize || 0) < TRADE_AGGREGATION_MIN_TOTAL_USD
        ) {
            Logger.info(
                `[${workerLabel}] buffering $${(activity.usdcSize || 0).toFixed(2)} ${activity.side} for ${activity.slug || activity.asset}`
            );
            addToAggregationBuffer(activity);
            continue;
        }

        try {
            await doTrading(clobClient, [activity], workerLabel);
        } catch (error) {
            Logger.error(`[${workerLabel}] Failed to execute trade: ${error}`);
        }
    }
};

const start = async () => {
    const clobClient = await createClobClient();
    Logger.success(`Worker ${workerLabel} CLOB client ready`);
    startAggregationFlusher(clobClient);
    dequeueLoop(clobClient);
};

if (!parentPort) {
    throw new Error('executor worker must have parentPort');
}

parentPort.on('message', (message: any) => {
    if (!message || typeof message !== 'object') return;
    const { type, payload } = message;
    if (type === 'activity' && payload) {
        localQueue.push(payload as QueueActivity);
    }
    if (type === 'shutdown') {
        isRunning = false;
        stopAggregationFlusher();
    }
});

start().catch((err) => {
    Logger.error(`Worker ${workerLabel} failed to start: ${err}`);
    process.exit(1);
});
