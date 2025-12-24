import axios from 'axios';
import fs from 'fs';
import path from 'path';
import moment from 'moment-timezone';
import { createObjectCsvWriter } from 'csv-writer';

const HISTORY_DAYS = (() => {
    const raw = process.env.HISTORY_DAYS;
    const value = raw ? Number(raw) : 30;
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : 30;
})();

const MAX_TRADES_PER_TRADER = (() => {
    const raw = process.env.HISTORY_MAX_TRADES;
    const value = raw ? Number(raw) : 20000;
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : 20000;
})();

const BATCH_SIZE = (() => {
    const raw = process.env.HISTORY_BATCH_SIZE;
    const value = raw ? Number(raw) : 100;
    return Number.isFinite(value) && value > 0 ? Math.min(Math.floor(value), 1000) : 100;
})();

const MAX_PARALLEL = (() => {
    const raw = process.env.HISTORY_MAX_PARALLEL;
    const value = raw ? Number(raw) : 4;
    return Number.isFinite(value) && value > 0 ? Math.min(Math.floor(value), 10) : 4;
})();

interface TradeApiResponse {
    id: string;
    timestamp: number;
    slug?: string;
    market?: string;
    asset: string;
    side: 'BUY' | 'SELL';
    price: number;
    usdcSize: number;
    size: number;
    outcome?: string;
}

interface CsvRecord {
    title: string;
    outcome: string;
    price: number;
    size: number;
    usdcsize: number;
    type: string;
    timestamp: string;
}

const fetchMarketTitle = async (marketId: string): Promise<string> => {
    try {
        const response = await axios.get(`https://gamma-api.polymarket.com/markets/${marketId}`, {
            timeout: 10000,
        });
        return response.data.title || marketId;
    } catch (error) {
        console.warn(`无法获取市场标题 for ${marketId}:`, error);
        return marketId;
    }
};

const getMarketTitles = async (trades: TradeApiResponse[]): Promise<Map<string, string>> => {
    const marketIds = new Set<string>();
    trades.forEach(trade => {
        if (trade.market) {
            marketIds.add(trade.market);
        }
    });

    const marketMap = new Map<string, string>();
    const promises = Array.from(marketIds).map(async (id) => {
        const title = await fetchMarketTitle(id);
        marketMap.set(id, title);
    });

    await Promise.all(promises);
    return marketMap;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchBatch = async (
    address: string,
    offset: number,
    limit: number
): Promise<TradeApiResponse[]> => {
    const response = await axios.get(
        `https://data-api.polymarket.com/activity?user=${address}&type=TRADE&limit=${limit}&offset=${offset}`,
        {
            timeout: 15000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
        }
    );

    return Array.isArray(response.data) ? response.data : [];
};

const fetchTradesForTrader = async (address: string, days: number, maxTrades: number): Promise<TradeApiResponse[]> => {
    console.log(`\n🚀 为 ${address} 下载历史数据 (最近 ${days} 天)`);
    const sinceTimestamp = Math.floor((Date.now() - days * 24 * 60 * 60 * 1000) / 1000);

    let offset = 0;
    let allTrades: TradeApiResponse[] = [];
    let hasMore = true;

    while (hasMore && allTrades.length < maxTrades) {
        const batchLimit = Math.min(BATCH_SIZE, maxTrades - allTrades.length);
        const batch = await fetchBatch(address, offset, batchLimit);

        if (batch.length === 0) {
            hasMore = false;
            break;
        }

        const filtered = batch.filter((trade) => trade.timestamp >= sinceTimestamp);
        allTrades = allTrades.concat(filtered);

        if (batch.length < batchLimit || filtered.length < batch.length) {
            hasMore = false;
        }

        offset += batchLimit;

        if (allTrades.length % (BATCH_SIZE * MAX_PARALLEL) === 0) {
            await sleep(150);
        }
    }

    const sorted = allTrades.sort((a, b) => a.timestamp - b.timestamp);
    console.log(`✓ 已获取 ${sorted.length} 笔交易`);
    return sorted;
};

const convertToCsvRecords = (trades: TradeApiResponse[]): CsvRecord[] => {
    return trades.map((trade) => ({
        title: trade.slug || trade.market || '',
        outcome: trade.outcome || '',
        price: trade.price,
        size: trade.size,
        usdcsize: trade.usdcSize,
        type: trade.side,
        timestamp: moment.tz(trade.timestamp * 1000, 'America/New_York').format('YYYY-MM-DD HH:mm:ss'),
    }));
};

const saveTradesToCsv = async (allTrades: TradeApiResponse[], addresses: string[], days: number) => {
    const csvRecords = convertToCsvRecords(allTrades);

    // Sort by timestamp descending
    csvRecords.sort((a, b) => moment(b.timestamp, 'YYYY-MM-DD HH:mm:ss').valueOf() - moment(a.timestamp, 'YYYY-MM-DD HH:mm:ss').valueOf());

    const outputDir = path.join(process.cwd(), 'trader_data_cache');
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    const today = new Date().toISOString().split('T')[0];
    const csvFile = path.join(outputDir, `historical_trades_${addresses.join('_').slice(0, 20)}_${days}d_${today}.csv`);

    const csvWriter = createObjectCsvWriter({
        path: csvFile,
        header: [
            { id: 'title', title: 'title' },
            { id: 'outcome', title: 'outcome' },
            { id: 'price', title: 'price' },
            { id: 'size', title: 'size' },
            { id: 'usdcsize', title: 'usdcsize' },
            { id: 'type', title: 'type' },
            { id: 'timestamp', title: 'timestamp' },
        ],
    });

    await csvWriter.writeRecords(csvRecords);
    console.log(`💾 已保存到 ${csvFile}`);
};

const parseArgs = () => {
    const args = process.argv.slice(2);
    let addresses: string[] = [];
    let days = HISTORY_DAYS;
    let maxTrades = MAX_TRADES_PER_TRADER;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--addresses' && args[i + 1]) {
            addresses = args[i + 1].split(',');
            i++;
        } else if (args[i] === '--days' && args[i + 1]) {
            days = Number(args[i + 1]);
            i++;
        } else if (args[i] === '--max-trades' && args[i + 1]) {
            maxTrades = Number(args[i + 1]);
            i++;
        }
    }

    return { addresses, days, maxTrades };
};

const chunk = <T>(array: T[], size: number): T[][] => {
    const result: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
        result.push(array.slice(i, i + size));
    }
    return result;
};

const main = async () => {
    const { addresses, days, maxTrades } = parseArgs();

    if (addresses.length === 0) {
        console.log('未指定地址。请使用 --addresses addr1,addr2,...');
        return;
    }

    console.log('📥 开始下载历史交易数据');
    console.log(`交易者数量: ${addresses.length}`);
    console.log(`期间: ${days} 天, 每个交易者最多 ${maxTrades} 笔交易`);

    const addressChunks = chunk(addresses, MAX_PARALLEL);
    let allTrades: TradeApiResponse[] = [];

    for (const chunkItem of addressChunks) {
        await Promise.all(
            chunkItem.map(async (address) => {
                try {
                    const trades = await fetchTradesForTrader(address, days, maxTrades);
                    allTrades = allTrades.concat(trades);
                } catch (error) {
                    console.error(`✗ 下载 ${address} 时出错:`, error);
                }
            })
        );
    }

    // Sort all trades by timestamp descending
    allTrades.sort((a, b) => b.timestamp - a.timestamp);

    await saveTradesToCsv(allTrades, addresses, days);

    console.log('\n✅ 下载和转换完成');
};

main();