import { ENV } from '../config/env';
import fetchData from '../utils/fetchData';
import Logger from '../utils/logger';
import { activityDedupCache, buildActivityDedupKey, QueueActivity } from './activityQueue';
import { publishActivityToWorkers } from './activityDistributor';

const USER_ADDRESSES = ENV.USER_ADDRESSES;
const TOO_OLD_SECONDS = ENV.TOO_OLD_SECONDS;
const FETCH_INTERVAL = ENV.FETCH_INTERVAL;

if (!USER_ADDRESSES || USER_ADDRESSES.length === 0) {
    throw new Error('USER_ADDRESSES is not defined or empty');
}

// In-memory cache for positions: key -> {data, timestamp}
// Expires after 1 minute
const positionCache = new Map<string, { data: any; timestamp: number }>();

const init = async () => {
    // Show your own positions first
    try {
        const myPositionsUrl = `https://data-api.polymarket.com/positions?user=${ENV.PROXY_WALLET}`;
        const myPositions = await fetchData(myPositionsUrl);

        // Get current USDC balance
        const getMyBalance = (await import('../utils/getMyBalance')).default;
        const currentBalance = await getMyBalance(ENV.PROXY_WALLET);

        if (Array.isArray(myPositions) && myPositions.length > 0) {
            // Calculate your overall profitability and initial investment
            let totalValue = 0;
            let initialValue = 0;
            let weightedPnl = 0;
            myPositions.forEach((pos: any) => {
                const value = pos.currentValue || 0;
                const initial = pos.initialValue || 0;
                const pnl = pos.percentPnl || 0;
                totalValue += value;
                initialValue += initial;
                weightedPnl += value * pnl;
            });
            const myOverallPnl = totalValue > 0 ? weightedPnl / totalValue : 0;

            // Get top 5 positions by profitability (PnL)
            const myTopPositions = myPositions
                .sort((a: any, b: any) => (b.percentPnl || 0) - (a.percentPnl || 0))
                .slice(0, 5);

            Logger.clearLine();
            Logger.myPositions(
                ENV.PROXY_WALLET,
                myPositions.length,
                myTopPositions,
                myOverallPnl,
                totalValue,
                initialValue,
                currentBalance
            );
        } else {
            Logger.clearLine();
            Logger.myPositions(ENV.PROXY_WALLET, 0, [], 0, 0, 0, currentBalance);
        }
    } catch (error) {
        Logger.error(`Failed to fetch your positions: ${error}`);
    }

};

const fetchTradeData = async (): Promise<number> => {
    
    // Clean up expired cache entries (older than 1 minute)
    const now = Date.now();
    for (const [key, value] of positionCache) {
        if (now - value.timestamp >= 60000) {
            positionCache.delete(key);
        }
    }

    let enqueuedCount = 0;

    for (const address of USER_ADDRESSES) {
        try {            
            // Fetch trade activities from Polymarket API (last 1 minute)
            const apiUrl = `https://data-api.polymarket.com/activity?user=${address}&type=TRADE`;
            const activities = await fetchData(apiUrl);
                        
            if (!Array.isArray(activities)) {
                Logger.warning(`[MONITOR] Invalid response for ${address.slice(0, 6)}...${address.slice(-4)}: not an array`);
                continue;
            }
            
            if (activities.length === 0) {
                Logger.info(`[MONITOR] No activities found for ${address.slice(0, 6)}...${address.slice(-4)}`);
                continue;
            }
            
            // Process each activity
            for (const activity of activities) {
                // Skip if too old
                // `TOO_OLD_SECONDS` is expressed in seconds (ENV). `activity.timestamp`
                // from the API is an epoch (seconds or milliseconds) or ISO string.
                // Convert `activity.timestamp` to milliseconds then compare age.
                const nowMs = Date.now();
                let activityMs = 0;
                if (typeof activity.timestamp === 'number') {
                    // heuristic: values > 1e12 are ms, otherwise seconds
                    activityMs = activity.timestamp > 1e12 ? activity.timestamp : activity.timestamp * 1000;
                } else if (typeof activity.timestamp === 'string') {
                    const parsed = Date.parse(activity.timestamp);
                    activityMs = isNaN(parsed) ? 0 : parsed;
                }
                const tooOldMs = TOO_OLD_SECONDS * 1000; // seconds -> ms
                if (activityMs === 0 || nowMs - activityMs > tooOldMs) {
                    // unknown timestamp or older than threshold
                    continue;
                }
                
                const detectedAt = Date.now();
                const queueActivity: QueueActivity = {
                    ...activity,
                    timestamp: activityMs,
                    userAddress: address,
                    _detectedAt: detectedAt,
                } as any;

                const dedupKey = buildActivityDedupKey(queueActivity);
                if (!activityDedupCache.checkAndRemember(dedupKey)) {
                    Logger.info(`[MONITOR] Duplicate activity detected, skipping: ${dedupKey}`);
                    continue;
                }

                const detectionLatency = detectedAt - activityMs;
                const beforePublish = Date.now();
                publishActivityToWorkers(queueActivity);
                enqueuedCount++;
                const publishLatency = Date.now() - beforePublish;

                Logger.info(
                    `[MONITOR] Activity detected for ${address.slice(0, 6)}...${address.slice(-4)} | TxHash: ${activity.transactionHash?.slice(0, 10)}... | Activity age: ${detectionLatency}ms | Publish time: ${publishLatency}ms`
                );
            }

            // Also fetch and update positions
            const positionsUrl = `https://data-api.polymarket.com/positions?user=${address}`;
            const positions = await fetchData(positionsUrl);

            if (Array.isArray(positions) && positions.length > 0) {
                for (const position of positions) {
                    const key = `${address}-${position.asset}-${position.conditionId}`;
                    const cached = positionCache.get(key);
                    const now = Date.now();
                    let needsUpdate = true;
                    if (cached && now - cached.timestamp < 60000) { // 1 minute
                        // Check if data is the same
                        if (JSON.stringify(cached.data) === JSON.stringify(position)) {
                            needsUpdate = false;
                        }
                    }
                    if (needsUpdate) {
                        positionCache.set(key, { data: position, timestamp: now });
                    }
                }
            }
        } catch (error) {
            Logger.error(
                `Error fetching data for ${address.slice(0, 6)}...${address.slice(-4)}: ${error}`
            );
        }
    }
    return enqueuedCount;
};

// Track if monitor should continue running
let isRunning = true;

/**
 * Stop the trade monitor gracefully
 */
export const stopTradeMonitor = () => {
    isRunning = false;
    Logger.info('Trade monitor shutdown requested...');
};

const tradeMonitor = async () => {
    await init();
    Logger.success(`Monitoring ${USER_ADDRESSES.length} trader(s) every ${FETCH_INTERVAL}s`);
    Logger.info(`[MONITOR] Too old threshold: ${TOO_OLD_SECONDS}s`);
    Logger.separator();

    let cycleCount = 0;
    while (isRunning) {
        cycleCount++;
        Logger.info(`[MONITOR] === Cycle #${cycleCount} started ===`);
        
        try {
            const enqueued = await fetchTradeData();
            Logger.info(`[MONITOR] Cycle #${cycleCount} enqueued: ${enqueued} activities`);
        } catch (error) {
            Logger.error(`[MONITOR] Error in fetch cycle: ${error}`);
        }
                
        if (!isRunning) break;
        await new Promise((resolve) => setTimeout(resolve, FETCH_INTERVAL * 1000));
    }

    Logger.info('[MONITOR] Trade monitor stopped');
};

export default tradeMonitor;
