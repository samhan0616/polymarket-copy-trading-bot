import { MessagePort, Worker } from 'worker_threads';
import Logger from '../utils/logger';
import { QueueActivity } from './activityQueue';

type WorkerEndpoint = MessagePort | Worker;

interface WorkerPort {
    id: number;
    endpoint: WorkerEndpoint;
}

const workerPorts: WorkerPort[] = [];
let roundRobinIndex = 0;
const backlog: QueueActivity[] = [];

const send = (endpoint: WorkerEndpoint, message: any) => {
    if ('postMessage' in endpoint) {
        endpoint.postMessage(message);
    }
};

export const registerWorkerEndpoint = (id: number, endpoint: WorkerEndpoint) => {
    workerPorts.push({ id, endpoint });
    Logger.success(`Worker #${id} registered for activity distribution`);
    flushBacklog();
};

export const unregisterWorkerEndpoint = (id: number) => {
    const idx = workerPorts.findIndex((w) => w.id === id);
    if (idx >= 0) {
        workerPorts.splice(idx, 1);
        Logger.warning(`Worker #${id} unregistered from activity distribution`);
    }
};

const pickWorker = (): WorkerPort | null => {
    if (workerPorts.length === 0) return null;
    const worker = workerPorts[roundRobinIndex % workerPorts.length];
    roundRobinIndex = (roundRobinIndex + 1) % workerPorts.length;
    return worker;
};

const flushBacklog = () => {
    if (backlog.length === 0) return;
    while (backlog.length > 0 && workerPorts.length > 0) {
        const activity = backlog.shift();
        if (!activity) break;
        const worker = pickWorker();
        if (worker) send(worker.endpoint, { type: 'activity', payload: activity });
    }
};

export const publishActivityToWorkers = (activity: QueueActivity) => {
    const worker = pickWorker();
    if (!worker) {
        backlog.push(activity);
        return;
    }
    send(worker.endpoint, { type: 'activity', payload: activity });
};

export const broadcastShutdown = () => {
    for (const { endpoint } of workerPorts) {
        send(endpoint, { type: 'shutdown' });
    }
};

// Test-only helper to reset distributor state between runs
export const resetDistributor = () => {
    workerPorts.length = 0;
    roundRobinIndex = 0;
    backlog.length = 0;
};
