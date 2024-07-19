use csv::ReaderBuilder;
use fasthash::murmur3;
use gnuplot::{AxesCommon, Figure, PlotOption::Caption};
use hashbrown::HashSet;
use lru::LruCache;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroUsize;
use std::{error::Error, sync::Arc};
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;

const NUM_CACHE_SIZE: u64 = 100;
type Key = u64;
const MODULUS: u64 = 1000;

fn hash(key: Key) -> u128 {
    murmur3::hash128(key.to_le_bytes())
}

pub trait Shards: Send {
    fn get_global_t(&self) -> u64;
    fn get_sampled_count(&self) -> u64;
    fn get_total_count(&self) -> u64;
    fn get_expected_count(&self) -> u64;

    fn get_correction(&self) -> i64 {
        self.get_expected_count() as i64 - self.get_sampled_count() as i64
    }

    fn get_rate(&self) -> f64 {
        self.get_global_t() as f64 / MODULUS as f64
    }

    fn sample(&mut self, access: &Key) -> bool;

    fn sample_key(&self, key: Key) -> Option<u64> {
        let t = (hash(key) % MODULUS as u128) as u64;

        match t < self.get_global_t() {
            true => Some(t),
            false => None,
        }
    }

    fn scale(&self, size: u64) -> u64 {
        (size as f64 * self.get_rate()) as u64
    }

    fn unscale(&self, size: u64) -> u64 {
        (size as f64 / self.get_rate()) as u64
    }

    fn get_removal(&mut self) -> Option<Key> {
        None
    }
}

pub struct ShardsFixedRate {
    global_t: u64,

    sampled_count: u64,
    total_count: u64,
}

impl ShardsFixedRate {
    #[allow(dead_code)]
    pub fn new(global_t: u64) -> Self {
        ShardsFixedRate {
            global_t,

            sampled_count: 0,
            total_count: 0,
        }
    }
}

impl Shards for ShardsFixedRate {
    fn get_global_t(&self) -> u64 {
        self.global_t
    }

    fn get_sampled_count(&self) -> u64 {
        self.sampled_count
    }

    fn get_total_count(&self) -> u64 {
        self.total_count
    }

    fn get_expected_count(&self) -> u64 {
        (self.get_rate() * self.total_count as f64) as u64
    }

    fn sample(&mut self, access: &Key) -> bool {
        self.total_count += 1;

        if self.sample_key(*access).is_none() {
            return false;
        }

        self.sampled_count += 1;

        true
    }
}

struct MiniSim {
    max_cache_size: u64,
    caches: Vec<LruCache<Key, ()>>,
    hits: Vec<u64>,
    access_count: u64,
    shards: Option<Box<dyn Shards>>,
    shards_global_t: u64,
}

fn get_caches(
    max_cache_size: u64,
    num_caches: u64,
    shards: &Option<Box<dyn Shards>>,
) -> Vec<LruCache<Key, ()>> {
    (1..=num_caches)
        .map(|i| {
            let mut cache_size = (i + 1) * (max_cache_size / num_caches as u64);

            if let Some(shards) = shards.as_ref() {
                cache_size = shards.scale(cache_size);
            }
            LruCache::new(NonZeroUsize::new(cache_size as usize).unwrap())
        })
        .collect()
}

impl MiniSim {
    pub fn new(max_cache_size: u64, shards: Option<Box<dyn Shards>>) -> Self {
        let caches = get_caches(max_cache_size, NUM_CACHE_SIZE, &shards);
        let shards_global_t = shards
            .as_ref()
            .map(|shards| shards.get_global_t())
            .unwrap_or(0);

        MiniSim {
            max_cache_size,
            caches,
            hits: vec![0; NUM_CACHE_SIZE as usize],
            access_count: 0,
            shards,
            shards_global_t,
        }
    }

    fn remove(&mut self, key: Key) {
        self.caches.par_iter_mut().for_each(|cache| {
            cache.pop(&key);
        });
    }

    fn clean(&mut self) {
        self.caches.par_iter_mut().for_each(|cache| {
            cache.clear();
        });
    }

    fn verify_shards(&mut self, key: Key) -> bool {
        if let Some(ref mut shards) = self.shards.as_mut() {
            if !shards.sample(&key) {
                return false;
            }

            if let Some(key) = shards.get_removal() {
                self.remove(key);
            }
        }
        true
    }

    fn process(&mut self, access: Key) {
        self.access_count += 1;

        for (i, cache) in self.caches.iter_mut().enumerate() {
            if cache.contains(&access) {
                self.hits[i] += 1;
                cache.get(&access);
            } else {
                cache.put(access, ());
            }
        }
    }

    fn handle(&mut self, access: Key) {
        if !self.verify_shards(access) {
            return;
        }

        self.process(access);
    }

    fn curve(&self) -> Vec<(f64, f64)> {
        let mut points = Vec::new();
        points.push((0.0, 1.0));
        for (i, hit) in self.hits.iter().enumerate() {
            let miss_ratio = 1.0 - *hit as f64 / self.access_count as f64;
            let cache_size = (i + 1) as f64 / NUM_CACHE_SIZE as f64;
            points.push((cache_size as f64, miss_ratio));
        }
        return points;
    }
}

#[derive(Debug, serde::Deserialize)]
struct AccessRecord {
    timestamp: u64,
    command: u8,
    key: u64,
    size: u32,
    ttl: u32,
}

fn draw(lines: &[Vec<(f64, f64)>], path: &str) {
    let mut fg = Figure::new();

    let width = 1920;
    let height = 1080;

    fg.set_title("Miss ratio curve");

    fg.axes2d()
        .set_x_label("Cache size", &[])
        .set_y_label("Miss ratio", &[])
        .lines(
            lines[0].iter().map(|(x, y)| *x),
            lines[0].iter().map(|(x, y)| *y),
            &[Caption("Fixed rate: 10%")],
        )
        .lines(
            lines[1].iter().map(|(x, y)| *x),
            lines[1].iter().map(|(x, y)| *y),
            &[Caption("Fixed rate: 50%")],
        )
        .lines(
            lines[2].iter().map(|(x, y)| *x),
            lines[2].iter().map(|(x, y)| *y),
            &[Caption("Simulated")],
        );

    fg.save_to_png(path, width, height).unwrap();
}
fn main() -> Result<(), Box<dyn Error>> {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    // 打开CSV文件
    let file = File::open("./data/test.csv")?;

    let reader = BufReader::new(file);

    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);

    let mut access_records = Vec::new();
    for result in rdr.deserialize() {
        let record: AccessRecord = result?;
        access_records.push(record);
    }
    debug_assert!(access_records.len() > 0);

    let mut working_set = HashSet::new();
    let mut keys = Vec::new();
    for record in access_records.iter() {
        working_set.insert(record.key);
        keys.push(record.key);
    }
    debug_assert!(working_set.len() < access_records.len());

    debug!("Access records: length: {}", access_records.len());
    debug!("First access record: {:?}", access_records[0]);
    debug!("Working set: length: {}", working_set.len());

    // 启动两个线程，一个是sim，一个是sim_without_sim
    let kyes = Arc::new(keys);
    let max_cache_size = working_set.len() as u64;
    let sim_handle = std::thread::spawn({
        let keys = Arc::clone(&kyes);
        move || {
            let shards: Option<Box<dyn Shards>> = Some(Box::new(ShardsFixedRate::new(100)));
            let start = std::time::Instant::now();
            let mut sim = MiniSim::new(max_cache_size, shards);
            for key in keys.iter() {
                sim.handle(*key);
            }
            debug!("Sim time: {:?}", start.elapsed());
            sim.curve()
        }
    });

    let sim_handle_50 = std::thread::spawn({
        let keys = Arc::clone(&kyes);
        move || {
            let shards: Option<Box<dyn Shards>> = Some(Box::new(ShardsFixedRate::new(500)));
            let start = std::time::Instant::now();
            let mut sim = MiniSim::new(max_cache_size, shards);
            for key in keys.iter() {
                sim.handle(*key);
            }
            debug!("Sim time: {:?}", start.elapsed());
            sim.curve()
        }
    });

    let sim_without_shards_handle = std::thread::spawn({
        let keys = Arc::clone(&kyes);
        move || {
            let shards: Option<Box<dyn Shards>> = None;
            let start = std::time::Instant::now();
            let mut sim_without_sim = MiniSim::new(max_cache_size, shards);
            for key in keys.iter() {
                sim_without_sim.handle(*key);
            }
            debug!("Sim without shards time: {:?}", start.elapsed());
            sim_without_sim.curve()
        }
    });

    let mut lines = Vec::new();
    lines.push(sim_handle.join().unwrap());
    lines.push(sim_handle_50.join().unwrap());
    lines.push(sim_without_shards_handle.join().unwrap());

    draw(&lines, "miss_ratio_curve.png");

    Ok(())
}
