use csv::ReaderBuilder;
use gnuplot::{
    AxesCommon, Figure,
    PlotOption::{Caption, Color},
};
use hashbrown::HashSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::num::NonZeroUsize;
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;

fn lru_miss_ratio(keys: &Vec<u64>, cache_size: NonZeroUsize) -> f64 {
    let mut cache = lru::LruCache::new(cache_size);
    let mut miss = 0;
    for key in keys.iter() {
        if cache.contains(key) {
            cache.get(key);
        } else {
            cache.put(*key, ());
            miss += 1;
        }
    }
    miss as f64 / keys.len() as f64
}

fn fifo_miss_ratio(keys: &Vec<u64>, cache_size: NonZeroUsize) -> f64 {
    let mut cache = VecDeque::with_capacity(cache_size.get());
    let mut cache_set = HashSet::with_capacity(cache_size.get());
    let mut miss = 0;

    for key in keys {
        if cache_set.contains(key) {
            // 如果缓存中已经存在该键，不做任何操作
            continue;
        }

        cache.push_back(*key);
        cache_set.insert(*key);
        miss += 1;

        if cache.len() > cache_size.get() {
            let removed_key = cache.pop_front().unwrap();
            cache_set.remove(&removed_key);
        }
    }

    miss as f64 / keys.len() as f64
}

#[derive(Debug, serde::Deserialize)]
struct AccessRecord {
    timestamp: u64,
    command: u8,
    key: u64,
    size: u32,
    ttl: u32,
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
    let file = File::open("./data/test_twitter.csv")?;

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

    // 缓存大小比例计算优化
    let step = 0.005;
    let max_cache_size_ratio = 1.0;
    let mut result_lru = Vec::new();
    let mut result_fifo = Vec::new();

    // 加入 (0,1)
    result_lru.push((0.0, 1.0));
    result_fifo.push((0.0, 1.0));

    for i in 1..=(max_cache_size_ratio / step) as usize {
        let cache_size_ratio = i as f64 * step;
        let cache_size = (cache_size_ratio * working_set.len() as f64).round() as usize; // 使用四舍五入确保整数

        // LRU
        let lru_ratio = lru_miss_ratio(&keys, NonZeroUsize::new(cache_size).unwrap());
        println!(
            "cache size: {}, lru miss ratio: {}",
            cache_size_ratio, lru_ratio
        );
        result_lru.push((cache_size_ratio, lru_ratio));

        // FIFO
        let fifo_ratio = fifo_miss_ratio(&keys, NonZeroUsize::new(cache_size).unwrap()); // 深拷贝keys
        println!(
            "cache size: {}, fifo miss ratio: {}",
            cache_size_ratio, fifo_ratio
        );
        result_fifo.push((cache_size_ratio, fifo_ratio));
    }

    // 绘制图像
    // 计算合适的图像尺寸
    let x_min = result_lru[0].0;
    let x_max = result_lru.last().unwrap().0;
    let y_min = result_lru
        .iter()
        .map(|(_, y)| *y)
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();
    let y_max = result_lru
        .iter()
        .map(|(_, y)| *y)
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap();

    let x_range = x_max - x_min;
    let y_range = y_max - y_min;

    // 根据数据范围设置图像尺寸
    let width = (x_range * 1500.0).round() as u32; // 根据x轴范围调整宽度
    let height = (y_range * 1500.0).round() as u32; // 根据y轴范围调整高度

    // 绘制图像
    let mut fg = Figure::new();
    fg.axes2d()
        .set_title("Miss Ratio Curve", &[])
        .set_x_label("Cache Size / Working Set Size", &[])
        .set_y_label("Miss Ratio", &[])
        .lines(
            result_lru.iter().map(|(x, _)| *x),
            result_lru.iter().map(|(_, y)| *y),
            &[Caption("LRU"), Color("blue")],
        )
        .lines(
            result_fifo.iter().map(|(x, _)| *x),
            result_fifo.iter().map(|(_, y)| *y),
            &[Caption("FIFO"), Color("red")],
        );
    fg.save_to_png("result.png", width, height).unwrap();

    Ok(())
}
