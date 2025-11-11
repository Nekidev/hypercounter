use std::{
    ops::{Add, BitAnd, BitOr, BitXor, Not, Sub},
    sync::atomic::{
        AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicU8, AtomicU16, AtomicU32,
        AtomicU64, AtomicUsize, Ordering,
    },
};

/// A trait representing atomic number types that can be used in `HyperCounter`.
pub trait AtomicNumber {
    type Primitive: Copy
        + Add<Output = Self::Primitive>
        + Sub<Output = Self::Primitive>
        + BitOr<Output = Self::Primitive>
        + BitXor<Output = Self::Primitive>
        + BitAnd<Output = Self::Primitive>
        + Not<Output = Self::Primitive>
        + Ord
        + PartialEq;
    const ZERO: Self::Primitive;

    fn new(value: Self::Primitive) -> Self;
    fn is_zero(&self, ordering: Ordering) -> bool;
    fn load(&self, ordering: Ordering) -> Self::Primitive;
    fn swap(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_add(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_sub(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_and(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_nand(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_or(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_xor(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_min(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;
    fn fetch_max(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive;

    fn primitive_wrapping_add(a: Self::Primitive, b: Self::Primitive) -> Self::Primitive;
    fn primitive_wrapping_sub(a: Self::Primitive, b: Self::Primitive) -> Self::Primitive;
}

macro_rules! impl_atomic_number {
    ($atomic_type:ty, $primitive:ty) => {
        impl AtomicNumber for $atomic_type {
            type Primitive = $primitive;
            const ZERO: Self::Primitive = 0;

            #[inline]
            fn new(value: Self::Primitive) -> Self {
                <$atomic_type>::new(value)
            }

            #[inline]
            fn is_zero(&self, ordering: Ordering) -> bool {
                self.load(ordering) == Self::ZERO
            }

            #[inline]
            fn load(&self, ordering: Ordering) -> Self::Primitive {
                self.load(ordering)
            }

            #[inline]
            fn swap(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.swap(val, ordering)
            }

            #[inline]
            fn fetch_add(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_add(val, ordering)
            }

            #[inline]
            fn fetch_sub(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_sub(val, ordering)
            }

            #[inline]
            fn fetch_and(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_and(val, ordering)
            }

            #[inline]
            fn fetch_nand(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_nand(val, ordering)
            }

            #[inline]
            fn fetch_or(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_or(val, ordering)
            }

            #[inline]
            fn fetch_xor(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_xor(val, ordering)
            }

            #[inline]
            fn fetch_min(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_min(val, ordering)
            }

            #[inline]
            fn fetch_max(&self, val: Self::Primitive, ordering: Ordering) -> Self::Primitive {
                self.fetch_max(val, ordering)
            }

            #[inline]
            fn primitive_wrapping_add(a: Self::Primitive, b: Self::Primitive) -> Self::Primitive {
                a.wrapping_add(b)
            }

            #[inline]
            fn primitive_wrapping_sub(a: Self::Primitive, b: Self::Primitive) -> Self::Primitive {
                a.wrapping_sub(b)
            }
        }
    };
}

impl_atomic_number!(AtomicUsize, usize);
impl_atomic_number!(AtomicIsize, isize);
impl_atomic_number!(AtomicU8, u8);
impl_atomic_number!(AtomicI8, i8);
impl_atomic_number!(AtomicU16, u16);
impl_atomic_number!(AtomicI16, i16);
impl_atomic_number!(AtomicU32, u32);
impl_atomic_number!(AtomicI32, i32);
impl_atomic_number!(AtomicU64, u64);
impl_atomic_number!(AtomicI64, i64);
