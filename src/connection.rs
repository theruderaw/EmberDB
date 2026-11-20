use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub struct ConnectionLimiter {
    active: AtomicUsize,
    max: usize,
}

pub struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
}

impl ConnectionLimiter {
    pub fn new(max: usize) -> Self {
        assert!(max > 0, "maximum clients must be positive");

        Self {
            active: AtomicUsize::new(0),
            max,
        }
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<ConnectionPermit> {
        let mut current = self.active.load(Ordering::Acquire);

        loop {
            if current >= self.max {
                return None;
            }

            match self.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ConnectionPermit {
                        limiter: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.limiter.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_connection_limit() {
        let limiter = Arc::new(ConnectionLimiter::new(2));

        let first = limiter.try_acquire().unwrap();
        let second = limiter.try_acquire().unwrap();

        assert_eq!(limiter.active(), 2);
        assert!(limiter.try_acquire().is_none());

        drop(first);

        assert_eq!(limiter.active(), 1);

        let third = limiter.try_acquire().unwrap();

        assert_eq!(limiter.active(), 2);

        drop(second);
        drop(third);

        assert_eq!(limiter.active(), 0);
    }
}
