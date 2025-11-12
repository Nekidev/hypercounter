use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Instant,
};

use hypercounter::HyperCounter;

fn main() {
    load_ops_per_second_single_threaded_fetch_add_single_key();
    load_ops_per_second_single_threaded_fetch_add_multi_key();
    load_ops_per_second_single_threaded_insert();
    load_ops_per_second_single_threaded_remove();
    load_ops_per_second_multi_threaded_fetch_add_single_key();
    load_ops_per_second_multi_threaded_fetch_add_multi_key();
    load_ops_per_second_multi_threaded_insert();
    load_ops_per_second_multi_threaded_remove();
}

fn load_ops_per_second_single_threaded_fetch_add_single_key() {
    let counter: HyperCounter<i32, AtomicI32> = HyperCounter::new();

    let now = Instant::now();
    let mut i = 0;

    loop {
        counter.fetch_add(1, 1, Ordering::Relaxed);
        i += 1;

        if now.elapsed().as_secs() >= 1 {
            break;
        }
    }

    println!("Single-threaded single-key load ops/sec: {i}");
}

fn load_ops_per_second_single_threaded_fetch_add_multi_key() {
    let counter: HyperCounter<i32, AtomicI32> = HyperCounter::new();

    let now = Instant::now();
    let mut i = 0;

    loop {
        let key = i % 1000;
        counter.fetch_add(key, 1, Ordering::Relaxed);
        i += 1;

        if now.elapsed().as_secs() >= 1 {
            break;
        }
    }

    println!("Single-threaded multi-key load ops/sec: {i}");
}

fn load_ops_per_second_single_threaded_insert() {
    let counter: HyperCounter<i32, AtomicI32> = HyperCounter::new();

    let now = Instant::now();
    let mut i = 0;

    loop {
        counter.fetch_add(i, 1, Ordering::Relaxed);
        i += 1;

        if now.elapsed().as_secs() >= 1 {
            break;
        }
    }

    println!("Single-threaded insert ops/sec: {i}");
}

fn load_ops_per_second_single_threaded_remove() {
    let counter: HyperCounter<i32, AtomicI32> = HyperCounter::new();

    for i in 0..2_000_000 {
        counter.fetch_add(i, 1, Ordering::Relaxed);
    }

    let now = Instant::now();
    let mut i = 0;

    loop {
        counter.fetch_sub(i, 1, Ordering::Relaxed);
        i += 1;

        if now.elapsed().as_secs() >= 1 {
            break;
        }
    }

    println!("Single-threaded remove ops/sec: {i}");
}

fn load_ops_per_second_multi_threaded_fetch_add_single_key() {
    let counter: Arc<HyperCounter<i32, AtomicI32>> = Arc::new(HyperCounter::new());

    let mut handles = vec![];

    for _ in 0..8 {
        let counter = counter.clone();

        let handle = std::thread::spawn(move || {
            let now = Instant::now();

            loop {
                counter.fetch_add(1, 1, Ordering::Relaxed);

                if now.elapsed().as_secs() >= 1 {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!(
        "Multi-threaded single-key load ops/sec: {}",
        counter.load(&1, Ordering::Relaxed)
    );
}

fn load_ops_per_second_multi_threaded_fetch_add_multi_key() {
    let counter: Arc<HyperCounter<i32, AtomicI32>> = Arc::new(HyperCounter::new());

    let mut handles = vec![];

    for thread_id in 0..8 {
        let counter = counter.clone();

        let handle = std::thread::spawn(move || {
            let now = Instant::now();

            for i in 0.. {
                let key = (i + thread_id) % 1000;
                counter.fetch_add(key, 1, Ordering::Relaxed);

                if now.elapsed().as_secs() >= 1 {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut total = 0;
    for key in 0..1000 {
        total += counter.load(&key, Ordering::Relaxed);
    }

    println!("Multi-threaded multi-key load ops/sec: {total}");
}

fn load_ops_per_second_multi_threaded_insert() {
    let counter: Arc<HyperCounter<i32, AtomicI32>> = Arc::new(HyperCounter::new());

    let mut handles = vec![];

    for thread_id in 0..8 {
        let counter = counter.clone();

        let handle = std::thread::spawn(move || {
            let now = Instant::now();

            for i in 0.. {
                let key = i + thread_id * 1_000_000;
                counter.fetch_add(key, 1, Ordering::Relaxed);

                if now.elapsed().as_secs() >= 1 {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total = counter.len();

    println!("Multi-threaded insert ops/sec: {total}");
}

fn load_ops_per_second_multi_threaded_remove() {
    let counter: Arc<HyperCounter<i32, AtomicI32>> = Arc::new(HyperCounter::new());

    for i in 0..8_000_000 {
        counter.fetch_add(i, 1, Ordering::Relaxed);
    }

    let mut handles = vec![];

    for thread_id in 0..8 {
        let counter = counter.clone();

        let handle = std::thread::spawn(move || {
            let now = Instant::now();

            for i in 0.. {
                let key = i + thread_id * 1_000_000;
                counter.fetch_sub(key, 1, Ordering::Relaxed);

                if now.elapsed().as_secs() >= 1 {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total = counter.len();

    println!("Multi-threaded remove ops/sec: {}", 8_000_000 - total);
}
