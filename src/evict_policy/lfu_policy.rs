use super::EvictPolicy;
use crate::Key;
use std::collections::{BTreeMap, HashMap};
pub struct LfuPolicy {
    capacity: u64,
    size: u64,
    key_to_freq_and_size: HashMap<Key, (u64, u64)>, // (frequency, size)
    freq_to_keys: BTreeMap<u64, Vec<Key>>,
}

impl EvictPolicy for LfuPolicy {
    fn new(capacity: u64) -> Self {
        LfuPolicy {
            capacity,
            size: 0,
            key_to_freq_and_size: HashMap::new(),
            freq_to_keys: BTreeMap::new(),
        }
    }

    fn get(&mut self, key: Key) -> Option<()> {
        if let Some((freq, _)) = self.key_to_freq_and_size.get_mut(&key) {
            // Remove key from current frequency
            if let Some(keys) = self.freq_to_keys.get_mut(freq) {
                keys.retain(|&k| k != key);
                if keys.is_empty() {
                    self.freq_to_keys.remove(freq);
                }
            }

            // Increment frequency
            *freq += 1;

            // Add key to new frequency
            self.freq_to_keys
                .entry(*freq)
                .or_insert_with(Vec::new)
                .push(key);

            Some(())
        } else {
            None
        }
    }

    fn put(&mut self, key: Key, size: u64) {
        if self.capacity == 0 || size > self.capacity {
            return;
        }

        // If key already exists, update its frequency
        if let Some((_, _)) = self.key_to_freq_and_size.get_mut(&key) {
            self.get(key);
            return;
        }

        // Evict least frequently used item(s)
        while self.size + size > self.capacity {
            if let Some((&least_freq, keys)) = self.freq_to_keys.iter_mut().next() {
                let evicted_keys: Vec<Key> = keys.drain(..).collect();
                for evicted_key in evicted_keys {
                    if let Some((_, evicted_size)) = self.key_to_freq_and_size.remove(&evicted_key)
                    {
                        self.size -= evicted_size;
                    }
                }
                self.freq_to_keys.remove(&least_freq);
            } else {
                break; // No more items to evict
            }

            if self.size + size <= self.capacity {
                break;
            }
        }

        // Add new key
        self.key_to_freq_and_size.insert(key, (1, size));
        self.freq_to_keys
            .entry(1)
            .or_insert_with(Vec::new)
            .push(key);
        self.size += size;
    }
}
