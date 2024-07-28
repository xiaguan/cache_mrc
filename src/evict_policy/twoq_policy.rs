use crate::Key;
use std::collections::{HashMap, VecDeque};

use super::EvictPolicy;

pub struct TwoQPolicy {
    hot: VecDeque<Key>,
    cold: VecDeque<Key>,
    cold_map: HashMap<Key, usize>,
    capacity: u64,
    size: u64,
    key_to_size: HashMap<Key, u64>,
}

impl EvictPolicy for TwoQPolicy {
    fn new(capacity: u64) -> Self {
        TwoQPolicy {
            hot: VecDeque::new(),
            cold: VecDeque::new(),
            cold_map: HashMap::new(),
            capacity,
            size: 0,
            key_to_size: HashMap::new(),
        }
    }

    fn get(&mut self, key: Key) -> Option<()> {
        if let Some(&idx) = self.cold_map.get(&key) {
            self.cold.remove(idx);
            self.cold_map.remove(&key);
            self.hot.push_front(key);
            self.update_cold_indices();
        } else if let Some(pos) = self.hot.iter().position(|k| k == &key) {
            self.hot.remove(pos);
            self.hot.push_front(key);
        } else {
            return None;
        }

        Some(())
    }

    fn put(&mut self, key: Key, size: u64) {
        if self.get(key).is_some() {
            // Key already exists, update its size
            if let Some(old_size) = self.key_to_size.insert(key, size) {
                self.size = self.size - old_size + size;
            }
            return;
        }

        // Remove items if necessary to make space
        while self.size + size > self.capacity {
            if let Some(evicted_key) = self.evict_one() {
                if let Some(evicted_size) = self.key_to_size.remove(&evicted_key) {
                    self.size -= evicted_size;
                }
            } else {
                // Can't make space, don't add the new item
                return;
            }
        }

        // Add new item
        self.size += size;
        self.key_to_size.insert(key.clone(), size);
        self.cold.push_front(key.clone());
        self.update_cold_indices();

        // Move from cold to hot if necessary
        if self.cold.len() as u64 > self.capacity / 2 {
            if let Some(old_key) = self.cold.pop_back() {
                self.cold_map.remove(&old_key);
                self.hot.push_front(old_key);
            }
        }
    }
}

impl TwoQPolicy {
    fn update_cold_indices(&mut self) {
        for (i, key) in self.cold.iter().enumerate() {
            self.cold_map.insert(key.clone(), i);
        }
    }

    fn evict_one(&mut self) -> Option<Key> {
        if let Some(key) = self.hot.pop_back() {
            Some(key)
        } else if let Some(key) = self.cold.pop_back() {
            self.cold_map.remove(&key);
            Some(key)
        } else {
            None
        }
    }
}
