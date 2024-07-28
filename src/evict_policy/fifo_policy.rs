use std::collections::VecDeque;

use hashbrown::HashMap;

use crate::Key;

use super::EvictPolicy;

// FIFO (First In First Out) Policy implementation
pub struct FifoPolicy {
    capacity: u64,
    size: u64,
    cache: HashMap<Key, u64>,
    queue: VecDeque<Key>,
}

impl EvictPolicy for FifoPolicy {
    fn new(capacity: u64) -> Self {
        Self {
            capacity,
            size: 0,
            cache: HashMap::new(),
            queue: VecDeque::new(),
        }
    }

    fn get(&mut self, key: Key) -> Option<()> {
        self.cache.get(&key).map(|_| ())
    }

    fn put(&mut self, key: Key, size: u64) {
        // Evict items if necessary
        while self.size + size > self.capacity {
            if let Some(old_key) = self.queue.pop_front() {
                if let Some(old_size) = self.cache.remove(&old_key) {
                    self.size -= old_size;
                }
            } else {
                break; // Prevent infinite loop
            }
        }

        self.cache.insert(key, size);
        self.queue.push_back(key);
        self.size += size;
    }
}
