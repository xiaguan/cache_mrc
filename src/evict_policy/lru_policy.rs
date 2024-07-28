use std::num::NonZeroUsize;

use crate::Key;

use super::EvictPolicy;

// LRU (Least Recently Used) Policy implementation
pub struct LruPolicy {
    capacity: u64,
    size: u64,
    cache: lru::LruCache<Key, u64>,
}

impl EvictPolicy for LruPolicy {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            size: 0,
            cache: lru::LruCache::new(NonZeroUsize::new(capacity as usize).unwrap()),
        }
    }

    fn get(&mut self, key: Key) -> Option<()> {
        self.cache.get(&key).map(|_| ())
    }

    fn put(&mut self, key: Key, size: u64) {
        // Evict items if necessary
        while self.size + size > self.capacity {
            if let Some((_, evicted_size)) = self.cache.pop_lru() {
                self.size -= evicted_size;
            } else {
                break;
            }
        }
        self.cache.put(key, size);
        self.size += size;
    }
}
