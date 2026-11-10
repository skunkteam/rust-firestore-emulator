# fifo-rwlock

## Overview
Crate root for `fifo-rwlock` (`Cargo.toml`), providing a strictly FIFO asynchronous reader-writer lock (`FifoRwLock`) built on `tokio::sync::oneshot` with support for priority-preserving read-to-write lock upgrades and atomic write-to-read lock downgrades.

## Subdirectories & Boundaries
- `src`: [./src/README.md](./src/README.md) - Core implementation of `FifoRwLock`, `ReadGuard`, `OwnedReadGuard`, `OwnedWriteGuard`, and the internal FIFO scheduling queue (`FifoRwLockQueue`).
