# Rust Kubernetes Lease Lock

A small library to use kubernetes lease in RAII fasion.

## Features

- RAII: when `LeaseLock` is dropped, the lock release is scheduled.
- While holding the lock, the lease is renewed in the background.
- This library uses fencing tokens, so concurrent acquires behave predictably and only a single holder at a time is guaranteed to hold the lock.

## Example

``` rust
use rust_kube_lease::LeaseLock;

....

let api = kube::Api::default_namespaced(kube::Client::try_default().await.unwrap());

let lock = LeaseLock::new(api, "my-lock");
{
    let _guard = lock.try_acquire("holder-1").await?.unwrap();
    // the lock is now acquired
}
// the lock is now being released
```
