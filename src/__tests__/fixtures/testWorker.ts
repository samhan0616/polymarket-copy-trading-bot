import { parentPort, workerData } from 'worker_threads';

const workerId = workerData?.workerId || 0;
let isRunning = true;

if (!parentPort) {
    throw new Error('Test worker must have parentPort');
}

// Simulate worker receiving and processing activities
parentPort.on('message', (message: any) => {
    console.log(`[TestWorker ${workerId}] Received message:`, message);
    
    if (!message || typeof message !== 'object') return;

    const { type, payload } = message;

    if (type === 'activity' && payload) {
        // Acknowledge receipt
        console.log(`[TestWorker ${workerId}] Sending acknowledgement`);
        parentPort?.postMessage({
            type: 'received',
            workerId,
            activity: payload,
        });
    }

    if (type === 'shutdown') {
        isRunning = false;
        parentPort?.postMessage({
            type: 'shutdown-ack',
            workerId,
        });
    }
});

// Keep worker alive
const keepAlive = setInterval(() => {
    if (!isRunning) {
        clearInterval(keepAlive);
        process.exit(0);
    }
}, 100);
