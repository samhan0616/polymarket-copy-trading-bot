import { ENV } from '../config/env';
import { getUserActivityModel, getUserPositionModel } from '../models/userHistory';
import fetchData from '../utils/fetchData';
import Logger from '../utils/logger';

const USER_ADDRESSES = ENV.USER_ADDRESSES;
const TOO_OLD_SECONDS = ENV.TOO_OLD_SECONDS;
const FETCH_INTERVAL = ENV.FETCH_INTERVAL;

if (!USER_ADDRESSES || USER_ADDRESSES.length === 0) {
    throw new Error('USER_ADDRESSES is not defined or empty');
}

// In-memory cache for positions: key -> {data, timestamp}
// Expires after 1 minute
const positionCache = new Map<string, { data: any; timestamp: number }>();

// Create activity and position models for each user
const userModels = USER_ADDRESSES.map((address) => ({
    address,
    UserActivity: getUserActivityModel(address),
    UserPosition: getUserPositionModel(address),
}));

const init = async () => {
    const counts: number[] = [];
    for (const { address, UserActivity } of userModels) {
        const count = await UserActivity.countDocuments();
        counts.push(count);
    }
    Logger.clearLine();
    Logger.dbConnection(USER_ADDRESSES, counts);

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

    // Show current positions count with details for traders you're copying
    const positionCounts: number[] = [];
    const positionDetails: any[][] = [];
    const profitabilities: number[] = [];
    for (const { address, UserPosition } of userModels) {
        const positions = await UserPosition.find().exec();
        positionCounts.push(positions.length);

        // Calculate overall profitability (weighted average by current value)
        let totalValue = 0;
        let weightedPnl = 0;
        positions.forEach((pos) => {
            const value = pos.currentValue || 0;
            const pnl = pos.percentPnl || 0;
            totalValue += value;
            weightedPnl += value * pnl;
        });
        const overallPnl = totalValue > 0 ? weightedPnl / totalValue : 0;
        profitabilities.push(overallPnl);

        // Get top 3 positions by profitability (PnL)
        const topPositions = positions
            .sort((a, b) => (b.percentPnl || 0) - (a.percentPnl || 0))
            .slice(0, 3)
            .map((p) => p.toObject());
        positionDetails.push(topPositions);
    }
    Logger.clearLine();
    Logger.tradersPositions(USER_ADDRESSES, positionCounts, positionDetails, profitabilities);
};

const fetchTradeData = async () => {
    const overallStart = Date.now();

    // Clean up expired cache entries (older than 1 minute)
    const now = Date.now();
    for (const [key, value] of positionCache) {
        if (now - value.timestamp >= 60000) {
            positionCache.delete(key);
        }
    }

    for (const { address, UserActivity, UserPosition } of userModels) {
        const userStart = Date.now();
        try {
            // Fetch trade activities from Polymarket API (last 1 minute)
            const startTs = Math.floor((Date.now() - 60000) / 1000); // 1 minute ago in seconds
            const apiUrl = `https://data-api.polymarket.com/activity?user=${address}&type=TRADE&sortBy=TIMESTAMP`;
            const apiStart = Date.now();
            const activities = await fetchData(apiUrl);
            if (!Array.isArray(activities) || activities.length === 0) {
                continue;
            }

            // Process each activity
            const processStart = Date.now();
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
                console.log('order cretated', (nowMs - activityMs) / 1000, 's ago');
                // Check if this trade already exists in database
                const existingActivity = await UserActivity.findOne({
                    transactionHash: activity.transactionHash,
                }).exec();

                if (existingActivity) {
                    continue; // Already processed this trade
                }

                // Save new trade to database
              
                const newActivity = new UserActivity({
                    proxyWallet: activity.proxyWallet,
                    timestamp: activity.timestamp,
                    conditionId: activity.conditionId,
                    type: activity.type,
                    size: activity.size,
                    usdcSize: activity.usdcSize,
                    transactionHash: activity.transactionHash,
                    price: activity.price,
                    asset: activity.asset,
                    side: activity.side,
                    outcomeIndex: activity.outcomeIndex,
                    title: activity.title,
                    slug: activity.slug,
                    icon: activity.icon,
                    eventSlug: activity.eventSlug,
                    outcome: activity.outcome,
                    name: activity.name,
                    pseudonym: activity.pseudonym,
                    bio: activity.bio,
                    profileImage: activity.profileImage,
                    profileImageOptimized: activity.profileImageOptimized,
                    bot: false,
                    botExcutedTime: 0,
                });

                await newActivity.save();
                Logger.info(`New trade detected for ${address.slice(0, 6)}...${address.slice(-4)}`);
            }

            // Also fetch and update positions
            const positionsUrl = `https://data-api.polymarket.com/positions?user=${address}`;
            const posApiStart = Date.now();
            const positions = await fetchData(positionsUrl);

            if (Array.isArray(positions) && positions.length > 0) {
                const posProcessStart = Date.now();
                const positionsToUpdate: any[] = [];
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
                        positionsToUpdate.push(position);
                        positionCache.set(key, { data: position, timestamp: now });
                    }
                }
                if (positionsToUpdate.length > 0) {
                    const bulkOps = positionsToUpdate.map(position => ({
                        updateOne: {
                            filter: { asset: position.asset, conditionId: position.conditionId },
                            update: {
                                $set: {
                                    proxyWallet: position.proxyWallet,
                                    asset: position.asset,
                                    conditionId: position.conditionId,
                                    size: position.size,
                                    avgPrice: position.avgPrice,
                                    initialValue: position.initialValue,
                                    currentValue: position.currentValue,
                                    cashPnl: position.cashPnl,
                                    percentPnl: position.percentPnl,
                                    totalBought: position.totalBought,
                                    realizedPnl: position.realizedPnl,
                                    percentRealizedPnl: position.percentRealizedPnl,
                                    curPrice: position.curPrice,
                                    redeemable: position.redeemable,
                                    mergeable: position.mergeable,
                                    title: position.title,
                                    slug: position.slug,
                                    icon: position.icon,
                                    eventSlug: position.eventSlug,
                                    outcome: position.outcome,
                                    outcomeIndex: position.outcomeIndex,
                                    oppositeOutcome: position.oppositeOutcome,
                                    oppositeAsset: position.oppositeAsset,
                                    endDate: position.endDate,
                                    negativeRisk: position.negativeRisk,
                                }
                            },
                            upsert: true
                        }
                    }));
                    await UserPosition.bulkWrite(bulkOps);
                }
            }
        } catch (error) {
            Logger.error(
                `Error fetching data for ${address.slice(0, 6)}...${address.slice(-4)}: ${error}`
            );
        }
    }
};

// Track if this is the first run
let isFirstRun = true;
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
    Logger.separator();

    // On first run, mark all existing historical trades as already processed
    if (isFirstRun) {
        Logger.info('First run: marking all historical trades as processed...');
        for (const { address, UserActivity } of userModels) {
            const count = await UserActivity.updateMany(
                { bot: false },
                { $set: { bot: true, botExcutedTime: 999 } }
            );
            if (count.modifiedCount > 0) {
                Logger.info(
                    `Marked ${count.modifiedCount} historical trades as processed for ${address.slice(0, 6)}...${address.slice(-4)}`
                );
            }
        }
        isFirstRun = false;
        Logger.success('\nHistorical trades processed. Now monitoring for new trades only.');
        Logger.separator();
    }

    while (isRunning) {
        const now = Date.now();
        await fetchTradeData();
        if (!isRunning) break;
        await new Promise((resolve) => setTimeout(resolve, FETCH_INTERVAL * 1000));
    }

    Logger.info('Trade monitor stopped');
};

export default tradeMonitor;
