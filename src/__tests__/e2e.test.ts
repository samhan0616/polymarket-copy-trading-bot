import { Worker } from 'worker_threads';
import path from 'path';
import { registerWorkerEndpoint, unregisterWorkerEndpoint, publishActivityToWorkers, broadcastShutdown, resetDistributor } from '../services/activityDistributor';
import { activityDedupCache } from '../services/activityQueue';

describe('E2E: Monitor -> Distributor -> Executor Workers', () => {
    let workers: Worker[] = [];
    const receivedActivities: Map<number, any[]> = new Map();

    beforeEach(() => {
        workers = [];
        receivedActivities.clear();
        resetDistributor();
    });

    afterEach(async () => {
        broadcastShutdown();
        for (const worker of workers) {
            await worker.terminate().catch(() => undefined);
        }
        workers = [];
    });

    it('should distribute activities to multiple workers in round-robin fashion', async () => {
        const workerCount = 3;
        const activitiesPerWorker = 2;

        // Create test workers
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        
        for (let i = 0; i < workerCount; i++) {
            const workerId = i + 1;
            const worker = new Worker(workerPath, {
                workerData: { workerId },
                execArgv: ['-r', 'ts-node/register'],
            });

            receivedActivities.set(workerId, []);

            worker.on('message', (msg) => {
                    console.log(`[Test] Worker ${workerId} sent message:`, msg);
                if (msg.type === 'received') {
                    receivedActivities.get(msg.workerId)?.push(msg.activity);
                        console.log(`[Test] Pushed activity, total: ${receivedActivities.get(workerId)?.length}`);
                }
            });

            await new Promise<void>((resolve) => {
                worker.on('online', () => {
                    registerWorkerEndpoint(workerId, worker);
                    resolve();
                });
            });

            workers.push(worker);
        }

        // Wait for workers to be ready
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Simulate monitor publishing activities
        const mockActivities = [];
        for (let i = 0; i < workerCount * activitiesPerWorker; i++) {
            const activity: any = {
                _id: `test-id-${i}`,
                userAddress: `0x${i.toString(16).padStart(40, '0')}`,
                transactionHash: `0x${i.toString(16).padStart(64, '0')}`,
                timestamp: Date.now(),
                conditionId: `condition-${i}`,
                asset: `asset-${i}`,
                side: i % 2 === 0 ? 'BUY' : 'SELL',
                usdcSize: 10 + i,
                price: 0.5 + i * 0.01,
                type: 'TRADE',
                size: 10,
                proxyWallet: '0x0000000000000000000000000000000000000000',
                outcomeIndex: 0,
                title: `Test Market ${i}`,
                slug: `test-market-${i}`,
                icon: '',
                eventSlug: `test-event-${i}`,
                outcome: 'Yes',
                name: 'Test User',
                pseudonym: 'testuser',
                bio: '',
                profileImage: '',
                profileImageOptimized: '',
                bot: false,
                botExcutedTime: 0,
            };
            mockActivities.push(activity);
            publishActivityToWorkers(activity);
        }

        // Wait for workers to process
        await new Promise((resolve) => setTimeout(resolve, 1000));

        // Verify round-robin distribution
        let totalReceived = 0;
        for (let workerId = 1; workerId <= workerCount; workerId++) {
            const received = receivedActivities.get(workerId) || [];
            expect(received.length).toBeGreaterThanOrEqual(activitiesPerWorker - 1);
            expect(received.length).toBeLessThanOrEqual(activitiesPerWorker + 1);
            totalReceived += received.length;
        }

        expect(totalReceived).toBe(mockActivities.length);
    });

    it('should handle worker registration and deduplication', async () => {
        const workerId = 1;
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        
        const worker = new Worker(workerPath, {
            workerData: { workerId },
            execArgv: ['-r', 'ts-node/register'],
        });

        receivedActivities.set(workerId, []);

        worker.on('message', (msg) => {
            console.log(`[Test-Dedup] Worker ${workerId} sent message:`, msg);
            if (msg.type === 'received') {
                receivedActivities.get(workerId)?.push(msg.activity);
                console.log(`[Test-Dedup] Pushed activity, total: ${receivedActivities.get(workerId)?.length}`);
            }
        });

        await new Promise<void>((resolve) => {
            worker.on('online', () => {
                registerWorkerEndpoint(workerId, worker);
                resolve();
            });
        });

        workers.push(worker);

        // Wait for worker to be fully ready (increased timeout)
        await new Promise((resolve) => setTimeout(resolve, 800));

        // Send the same activity twice (simulate duplicate)
        const activity: any = {
            _id: 'test-dedup-1',
            userAddress: '0x1234567890123456789012345678901234567890',
            transactionHash: '0xabc123',
            timestamp: Date.now(),
            conditionId: 'condition-1',
            asset: 'asset-1',
            side: 'BUY',
            usdcSize: 100,
            price: 0.75,
            type: 'TRADE',
            size: 100,
            proxyWallet: '0x0000000000000000000000000000000000000000',
            outcomeIndex: 0,
            title: 'Test Market',
            slug: 'test-market',
            icon: '',
            eventSlug: 'test-event',
            outcome: 'Yes',
            name: 'Test User',
            pseudonym: 'testuser',
            bio: '',
            profileImage: '',
            profileImageOptimized: '',
            bot: false,
            botExcutedTime: 0,
        };

        // Test deduplication at monitor level
        const dedupKey1 = activity.transactionHash;
        const isNew1 = activityDedupCache.checkAndRemember(dedupKey1);
        expect(isNew1).toBe(true);

        const isNew2 = activityDedupCache.checkAndRemember(dedupKey1);
        expect(isNew2).toBe(false);

        // Only send once (dedup worked)
        if (isNew1) {
            publishActivityToWorkers(activity);
        }

        await new Promise((resolve) => setTimeout(resolve, 800));

        const received = receivedActivities.get(workerId) || [];
        expect(received.length).toBe(1);
    });

    it('should handle worker shutdown gracefully', async () => {
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        const worker = new Worker(workerPath, {
            workerData: { workerId: 1 },
            execArgv: ['-r', 'ts-node/register'],
        });

        let shutdownReceived = false;
        worker.on('message', (msg) => {
            if (msg.type === 'shutdown-ack') {
                shutdownReceived = true;
            }
        });

        await new Promise<void>((resolve) => {
            worker.on('online', () => {
                registerWorkerEndpoint(1, worker);
                resolve();
            });
        });

        workers.push(worker);

        await new Promise((resolve) => setTimeout(resolve, 300));

        broadcastShutdown();

        await new Promise((resolve) => setTimeout(resolve, 500));

        expect(shutdownReceived).toBe(true);
    });

    it('should buffer activities when no workers are available', async () => {
        // Publish activity before any workers are registered
        const activity: any = {
            _id: 'test-buffered-1',
            userAddress: '0x1234567890123456789012345678901234567890',
            transactionHash: '0xbuffered',
            timestamp: Date.now(),
            conditionId: 'condition-buffered',
            asset: 'asset-buffered',
            side: 'BUY',
            usdcSize: 50,
            price: 0.6,
            type: 'TRADE',
            size: 50,
            proxyWallet: '0x0000000000000000000000000000000000000000',
            outcomeIndex: 0,
            title: 'Test Market Buffered',
            slug: 'test-market-buffered',
            icon: '',
            eventSlug: 'test-event-buffered',
            outcome: 'Yes',
            name: 'Test User',
            pseudonym: 'testuser',
            bio: '',
            profileImage: '',
            profileImageOptimized: '',
            bot: false,
            botExcutedTime: 0,
        };

        publishActivityToWorkers(activity);

        // Now register a worker
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        const worker = new Worker(workerPath, {
            workerData: { workerId: 1 },
            execArgv: ['-r', 'ts-node/register'],
        });

        receivedActivities.set(1, []);

        worker.on('message', (msg) => {
            if (msg.type === 'received') {
                receivedActivities.get(1)?.push(msg.activity);
            }
        });

        await new Promise<void>((resolve) => {
            worker.on('online', () => {
                registerWorkerEndpoint(1, worker);
                resolve();
            });
        });

        workers.push(worker);

        // Wait for buffered activity to be flushed (increased timeout)
        await new Promise((resolve) => setTimeout(resolve, 800));

        const received = receivedActivities.get(1) || [];
        expect(received.length).toBe(1);
        expect(received[0].transactionHash).toBe('0xbuffered');
    });

    it('should deliver all published activities to workers', async () => {
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        const workerCount = 2;
        const totalActivities = 6;

        for (let i = 0; i < workerCount; i++) {
            const workerId = i + 1;
            const worker = new Worker(workerPath, {
                workerData: { workerId },
                execArgv: ['-r', 'ts-node/register'],
            });

            receivedActivities.set(workerId, []);

            worker.on('message', (msg) => {
                if (msg.type === 'received') {
                    receivedActivities.get(workerId)?.push(msg.activity);
                    console.log(`[CountTest] Worker ${workerId} received, total=${receivedActivities.get(workerId)?.length}`);
                }
            });

            await new Promise<void>((resolve) => {
                worker.on('online', () => {
                    registerWorkerEndpoint(workerId, worker);
                    resolve();
                });
            });

            workers.push(worker);
        }

        // Allow workers to settle before publishing
        await new Promise((resolve) => setTimeout(resolve, 200));

        const activities = Array.from({ length: totalActivities }).map((_, idx) => ({
            _id: `count-${idx}`,
            userAddress: `0x${idx.toString(16).padStart(40, '0')}`,
            transactionHash: `0x${idx.toString(16).padStart(64, '0')}`,
            timestamp: Date.now(),
            conditionId: `condition-${idx}`,
            asset: `asset-${idx}`,
            side: idx % 2 === 0 ? 'BUY' : 'SELL',
            usdcSize: 100 + idx,
            price: 0.5 + idx * 0.01,
            type: 'TRADE',
            size: 10,
            proxyWallet: '0x0000000000000000000000000000000000000000',
            outcomeIndex: 0,
            title: 'Test Market',
            slug: 'test-market',
            icon: '',
            eventSlug: 'test-event',
            outcome: 'Yes',
            name: 'Test User',
            pseudonym: 'testuser',
            bio: '',
            profileImage: '',
            profileImageOptimized: '',
            bot: false,
            botExcutedTime: 0,
        }));

        activities.forEach((activity, idx) => {
            console.log(`[CountTest] Publishing activity ${idx}`);
            publishActivityToWorkers(activity as any);
        });

        await new Promise((resolve) => setTimeout(resolve, 1200));

        const totalReceived = Array.from(receivedActivities.values()).reduce((sum, arr) => sum + arr.length, 0);
        expect(totalReceived).toBe(totalActivities);
    });

    it('should deliver activities with same userAddress but different transactionHash', async () => {
        const workerPath = path.join(__dirname, './fixtures/testWorker.ts');
        const workerCount = 2;
        const totalActivities = 5;
        const sameUserAddress = '0x1234567890123456789012345678901234567890';

        for (let i = 0; i < workerCount; i++) {
            const workerId = i + 1;
            const worker = new Worker(workerPath, {
                workerData: { workerId },
                execArgv: ['-r', 'ts-node/register'],
            });

            receivedActivities.set(workerId, []);

            worker.on('message', (msg) => {
                if (msg.type === 'received') {
                    receivedActivities.get(workerId)?.push(msg.activity);
                    console.log(`[SameUserTest] Worker ${workerId} received activity with tx=${msg.activity.transactionHash}`);
                }
            });

            await new Promise<void>((resolve) => {
                worker.on('online', () => {
                    registerWorkerEndpoint(workerId, worker);
                    resolve();
                });
            });

            workers.push(worker);
        }

        // Allow workers to settle before publishing
        await new Promise((resolve) => setTimeout(resolve, 200));

        const activities = Array.from({ length: totalActivities }).map((_, idx) => ({
            _id: `same-user-${idx}`,
            userAddress: sameUserAddress,
            transactionHash: `0x${(1000 + idx).toString(16)}`, // Different tx hashes
            timestamp: Date.now() + idx,
            conditionId: `condition-${idx}`,
            asset: `asset-${idx}`,
            side: idx % 2 === 0 ? 'BUY' : 'SELL',
            usdcSize: 100 + idx,
            price: 0.5 + idx * 0.01,
            type: 'TRADE',
            size: 10,
            proxyWallet: '0x0000000000000000000000000000000000000000',
            outcomeIndex: 0,
            title: 'Test Market',
            slug: 'test-market',
            icon: '',
            eventSlug: 'test-event',
            outcome: 'Yes',
            name: 'Test User',
            pseudonym: 'testuser',
            bio: '',
            profileImage: '',
            profileImageOptimized: '',
            bot: false,
            botExcutedTime: 0,
        }));

        activities.forEach((activity) => publishActivityToWorkers(activity as any));

        await new Promise((resolve) => setTimeout(resolve, 1000));

        const totalReceived = Array.from(receivedActivities.values()).reduce((sum, arr) => sum + arr.length, 0);
        expect(totalReceived).toBe(totalActivities);

        // Verify all activities have the same userAddress
        Array.from(receivedActivities.values()).forEach((activityList) => {
            activityList.forEach((activity) => {
                expect(activity.userAddress).toBe(sameUserAddress);
            });
        });

        // Verify all transactionHashes are unique
        const allTxHashes = Array.from(receivedActivities.values())
            .flat()
            .map((a) => a.transactionHash);
        const uniqueTxHashes = new Set(allTxHashes);
        expect(uniqueTxHashes.size).toBe(totalActivities);
    });
});
