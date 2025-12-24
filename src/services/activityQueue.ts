import { ENV } from '../config/env';
import { UserActivityInterface } from '../interfaces/User';
import Logger from '../utils/logger';

export interface QueueActivity extends UserActivityInterface {
    userAddress: string;
}

class InMemoryDedupCache {
    private cache = new Map<string, number>();
    private readonly ttlMs: number;
    private readonly maxEntries: number;

    constructor(ttlMs: number, maxEntries = 5000) {
        this.ttlMs = Math.max(ttlMs, 1);
        this.maxEntries = Math.max(maxEntries, 1);
    }

    checkAndRemember(key: string): boolean {
        const now = Date.now();
        this.evictExpired(now);
        if (this.cache.has(key)) return false;

        this.cache.set(key, now);
        if (this.cache.size > this.maxEntries) {
            const oldestKey = this.cache.keys().next().value;
            if (oldestKey) this.cache.delete(oldestKey);
        }
        return true;
    }

    size(): number {
        this.evictExpired(Date.now());
        return this.cache.size;
    }

    private evictExpired(now: number): void {
        for (const [key, timestamp] of this.cache.entries()) {
            if (now - timestamp > this.ttlMs) {
                this.cache.delete(key);
            }
        }
    }
}

type ResolveFn<T> = (value: T | null) => void;

class AsyncActivityQueue<T> {
    private queue: T[] = [];
    private waiters: ResolveFn<T>[] = [];
    private closed = false;

    enqueue(item: T): void {
        if (this.closed) {
            Logger.warning('Attempted to enqueue into a closed activity queue. Dropping item.');
            return;
        }

        if (this.waiters.length > 0) {
            const resolve = this.waiters.shift();
            resolve?.(item);
            return;
        }

        this.queue.push(item);
    }

    async dequeue(options?: { timeoutMs?: number; signal?: AbortSignal }): Promise<T | null> {
        const timeoutMs = options?.timeoutMs ?? 1000;

        if (this.queue.length > 0) {
            return this.queue.shift() as T;
        }

        if (this.closed) return null;

        return await new Promise<T | null>((resolve) => {
            let settled = false;
            const settle = (value: T | null) => {
                if (settled) return;
                settled = true;
                cleanup();
                resolve(value);
            };

            const onAbort = () => settle(null);
            const cleanup = () => {
                if (timeoutId) clearTimeout(timeoutId);
                if (options?.signal) options.signal.removeEventListener('abort', onAbort);
            };

            const timeoutId = timeoutMs > 0 ? setTimeout(() => settle(null), timeoutMs) : undefined;
            this.waiters.push(settle);

            if (options?.signal) {
                options.signal.addEventListener('abort', onAbort, { once: true });
            }
        });
    }

    close(): void {
        this.closed = true;
        while (this.waiters.length > 0) {
            const resolve = this.waiters.shift();
            resolve?.(null);
        }
    }

    size(): number {
        return this.queue.length;
    }

    isClosed(): boolean {
        return this.closed;
    }
}

export const activityQueue = new AsyncActivityQueue<QueueActivity>();
export const activityDedupCache = new InMemoryDedupCache(
    Math.max(ENV.DEDUP_CACHE_TTL_SECONDS, 1) * 1000,
    5000
);

export const buildActivityDedupKey = (activity: QueueActivity): string => {
    if (activity.transactionHash) {
        return activity.transactionHash.toLowerCase();
    }

    return [
        activity.userAddress,
        activity.conditionId,
        activity.timestamp,
        activity.side,
        activity.usdcSize,
        activity.price,
    ].join(':');
};
