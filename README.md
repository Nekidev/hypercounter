# HyperCounter

An atomic, lock-free, hash map-like counter structure.

## Example

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use hypercounter::HyperCounter;

let counter: HyperCounter<String, AtomicUsize> = HyperCounter::new();

counter.fetch_add("example_key".to_string(), 1, Ordering::Relaxed);
counter.fetch_sub("example_key".to_string(), 1, Ordering::Relaxed);
```

## Documentation

To read the full documentation, visit [docs.rs/hypercounter](https://docs.rs/hypercounter).
