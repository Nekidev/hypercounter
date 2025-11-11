use std::{
    sync::atomic::{AtomicI32, Ordering},
    time::Instant,
};

use hypercounter::HyperCounter;

fn main() {
    load_ops_per_second_single_threaded_fetch_add_single_key();
    load_ops_per_second_single_threaded_fetch_add_multi_key();
    load_ops_per_second_single_threaded_insert();
    load_ops_per_second_single_threaded_remove();
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