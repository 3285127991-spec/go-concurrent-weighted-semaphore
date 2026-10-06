# weightedsemaphore

一个纯 Go 标准库实现的并发加权信号量：严格 FIFO 等待队列、支持 context 取消、
非阻塞 TryAcquire、Close 语义。无第三方依赖。

## 特性

- `Acquire(ctx, n)`：资源足够时立即获取，否则进入 FIFO 队列等待；后来的请求不能越过队首。
- `TryAcquire(n)`：不阻塞；仅在无更早等待者且资源足够时成功。
- `Release(n)`：归还资源并按 FIFO 顺序唤醒可满足等待者，超额释放返回错误。
- 等待中的 context 被取消时安全出队，不影响其他等待者。
- `Close` 后新的获取返回 `ErrClosed`，所有等待者被唤醒并返回该错误；已获得的资源仍可正常 `Release`。
- `Capacity` / `Available` / `Waiters` 查询，全部并发安全。

## API

```go
s, err := weightedsemaphore.New(8)        // capacity 必须为正整数
err  = s.Acquire(ctx, 3)                  // n 必须为正且 <= capacity
ok   := s.TryAcquire(2)
err  = s.Release(3)
cap  := s.Capacity()
free := s.Available()
n    := s.Waiters()
s.Close()                                 // 幂等
```

错误：`ErrInvalidN`（n 非正或超过 capacity）、`ErrOverRelease`（超额释放）、
`ErrClosed`（已关闭）。`Acquire` 还可能返回 `ctx.Err()`。

## 运行演示

```sh
go run ./cmd/demo
```

演示依次覆盖：立即 Acquire、等待阻塞、Release 唤醒、TryAcquire 公平性、Close。

## 运行测试

```sh
go test -race ./...
```

共 10 个测试：立即获取、阻塞后唤醒、FIFO 顺序、TryAcquire 公平性、context 取消、
非法 n、超额释放、Close、状态查询、基础并发场景。
