# E2E Test Suite

## Overview
端到端测试验证完整的活动流：monitor 检测 → 分发器路由 → executor workers 处理。

## Test Coverage

### 1. Round-Robin Distribution (`e2e.test.ts`)
测试多个 worker 线程按轮询方式接收活动：
- 启动 3 个 worker 线程
- 发送 6 个模拟交易活动
- 验证每个 worker 收到约 2 个活动（±1 容差）
- 确认总接收数 = 总发送数

### 2. Deduplication (`e2e.test.ts`)
测试去重机制防止重复处理：
- 使用相同 `transactionHash` 发送活动两次
- 验证 `activityDedupCache.checkAndRemember()` 返回 true/false
- 确认 worker 只收到一个活动实例

### 3. Graceful Shutdown (`e2e.test.ts`)
测试优雅关闭流程：
- 注册 worker 并监听 shutdown 消息
- 调用 `broadcastShutdown()`
- 验证 worker 收到 shutdown 信号并确认（`shutdown-ack`）

### 4. Backlog Buffering (`e2e.test.ts`)
测试无 worker 时的缓冲机制：
- 在注册 worker 之前发送活动
- 注册 worker 后验证缓冲的活动被自动刷新
- 确认 worker 收到之前缓冲的活动

## Running Tests

```bash
# Run all tests
npm test

# Run e2e tests only
npm test -- e2e.test.ts

# Run with coverage
npm test -- --coverage
```

## Test Fixtures

### `fixtures/testWorker.ts`
轻量级测试 worker，模拟真实 executor worker 的消息处理：
- 接收 `activity` 消息并确认接收
- 响应 `shutdown` 消息
- 无下单逻辑，纯粹用于测试线程通信

## Architecture Verified

```
Monitor (Main Thread)
  ↓ publishActivityToWorkers()
Distributor (Main Thread)
  ↓ Round-robin selection
  ↓ worker.postMessage() — 序列化
  ↓ [Message Channel]
  ↓ parentPort.on('message') — 反序列化
Worker 1, 2, 3... (Separate Threads)
  ↓ Local queue
  ↓ Process activity
```

## Thread Safety Guarantee
- **无共享内存**：每个线程独立内存空间
- **消息传递**：数据通过 `postMessage()` 深拷贝
- **去重在主线程**：避免跨线程竞争
- **本地队列**：每个 worker 维护独立队列

## Expected Results
所有测试应通过，验证：
✓ 活动正确分发到多个 worker
✓ 去重机制工作正常
✓ 关闭信号正确广播
✓ 无 worker 时自动缓冲并后续刷新
