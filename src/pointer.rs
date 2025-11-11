use std::sync::{
    Arc,
    atomic::{AtomicPtr, Ordering},
};

/// A safe atomic pointer that manages [`Arc<T>`] references.
pub struct SafeAtomicPtr<T> {
    ptr: AtomicPtr<T>,
}

// TODO: Implement ABA protection.
impl<T> SafeAtomicPtr<T> {
    /// Creates a new [`SafeAtomicPtr`] pointing to the given [`Arc<T>`].
    ///
    /// Arguments:
    /// * `value` - The initial value to point to.
    ///
    /// Returns:
    /// * [`SafeAtomicPtr<T>`] - A new instance of [`SafeAtomicPtr`].
    pub fn new(value: Arc<T>) -> Self {
        Self {
            ptr: AtomicPtr::new(Arc::into_raw(value) as *mut _),
        }
    }

    /// Creates a new [`SafeAtomicPtr`] initialized to null.
    ///
    /// Returns:
    /// * [`SafeAtomicPtr<T>`] - A new instance of [`SafeAtomicPtr`] with a null pointer.
    pub fn null() -> Self {
        Self {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Loads the current [`Arc<T>`] from the atomic pointer.
    ///
    /// Arguments:
    /// * `order` - The memory ordering for the load operation.
    ///
    /// Returns:
    /// * [`Option<Arc<T>>`] - The current value pointed to, or [`None`] if the pointer is null.
    pub fn load(&self, order: Ordering) -> Option<Arc<T>> {
        let raw_ptr = self.ptr.load(order);

        if raw_ptr.is_null() {
            None
        } else {
            let arc = unsafe { Arc::from_raw(raw_ptr) };

            // Clone it to give the caller a safe Arc
            let ret = arc.clone();

            // Forget the temporary so we don't decrement the ref count
            std::mem::forget(arc);

            Some(ret)
        }
    }

    /// Swaps the current pointer with a new [`Arc<T>`].
    ///
    /// Arguments:
    /// * `new_value` - The new value to point to. Use [`None`] to set the pointer to null.
    /// * `order` - The memory ordering for the swap operation.
    pub fn swap(&self, new_value: Option<Arc<T>>, order: Ordering) -> Option<Arc<T>> {
        let new_raw = if let Some(new_value) = new_value {
            Arc::into_raw(new_value) as *mut _
        } else {
            std::ptr::null_mut()
        };
        let old_raw = self.ptr.swap(new_raw, order);

        if !old_raw.is_null() {
            Some(unsafe { Arc::from_raw(old_raw) })
        } else {
            None
        }
    }

    /// Compares and exchanges the current pointer with a new [`Arc<T>`] if it matches the
    /// expected current value.
    ///
    /// Arguments:
    /// * `current` - The expected current value. Use [`None`] to expect a null pointer.
    /// * `new` - The new value to point to. Use [`None`] to set the pointer to null.
    /// * `success` - The memory ordering for a successful compare-and-exchange operation.
    /// * `failure` - The memory ordering for a failed compare-and-exchange operation.
    ///
    /// Returns:
    /// * `Ok(Option<Arc<T>>)` - The previous value if the exchange was successful.
    /// * `Err(Option<Arc<T>>)` - The actual current value if the exchange failed.
    pub fn compare_exchange(
        &self,
        current: Option<&Arc<T>>,
        new: Option<Arc<T>>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Option<Arc<T>>, Option<Arc<T>>> {
        let current_raw = match current {
            Some(arc) => Arc::as_ptr(arc) as *mut _,
            None => std::ptr::null_mut(),
        };

        let new_raw = match new {
            Some(arc) => Arc::into_raw(arc) as *mut _,
            None => std::ptr::null_mut(),
        };

        match self
            .ptr
            .compare_exchange(current_raw, new_raw, success, failure)
        {
            Ok(old_raw) => {
                // CAS succeeded
                if !old_raw.is_null() {
                    let old_raw = unsafe { Arc::from_raw(old_raw) };

                    return Ok(Some(old_raw));
                }
                Ok(None) // no previous value to return, since we replaced it
            }
            Err(actual_raw) => {
                // CAS failed — need to undo Arc::into_raw() since new was not used
                if !new_raw.is_null() {
                    unsafe { drop(Arc::from_raw(new_raw)) };
                }

                // Return the actual current Arc<T> at the pointer for convenience
                let actual = if actual_raw.is_null() {
                    None
                } else {
                    let arc = unsafe { Arc::from_raw(actual_raw) };
                    let cloned = arc.clone();
                    std::mem::forget(arc);
                    Some(cloned)
                };

                Err(actual)
            }
        }
    }
}

impl<T> Drop for SafeAtomicPtr<T> {
    fn drop(&mut self) {
        let raw = self.ptr.load(Ordering::Relaxed);

        if !raw.is_null() {
            // Reconstruct Arc and drop it immediately
            unsafe { Arc::from_raw(raw) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::Ordering};

    struct TestData(Arc<Mutex<bool>>);

    impl Drop for TestData {
        fn drop(&mut self) {
            let mut v = self.0.lock().unwrap();
            *v = true;
        }
    }

    #[test]
    fn test_dropping() {
        let drop_check = Arc::new(Mutex::new(false));

        let atomic_ptr = SafeAtomicPtr::new(Arc::new(TestData(drop_check.clone())));

        let loaded = atomic_ptr.load(Ordering::SeqCst).unwrap();
        assert!(!(*loaded.0.lock().unwrap()));

        drop(atomic_ptr);

        assert!(!(*loaded.0.lock().unwrap()));

        drop(loaded);
        
        assert!(*drop_check.lock().unwrap());
    }
}
