use crate::Key;

mod fifo_policy;
mod lfu_policy;
mod lru_policy;
mod twoq_policy;
pub use fifo_policy::FifoPolicy;
pub use lfu_policy::LfuPolicy;
pub use lru_policy::LruPolicy;
pub use twoq_policy::TwoQPolicy;
// Define the EvictPolicy trait
pub trait EvictPolicy: Send {
    fn new(capacity: u64) -> Self;
    fn get(&mut self, key: Key) -> Option<()>;
    fn put(&mut self, key: Key, size: u64);
}
