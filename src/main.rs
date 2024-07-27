use csv::ReaderBuilder;

use evict_policy::{EvictPolicy, LruPolicy};
use gnuplot::{AxesCommon, Figure, PlotOption::Caption};
use hashbrown::HashSet;

use minisim::MiniSim;
use shards::ShardsFixedRate;
use std::fs::File;
use std::io::BufReader;
use std::thread;
use std::{error::Error, sync::Arc};
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;

mod evict_policy;
mod minisim;
mod shards;

const NUM_CACHE_SIZE: u64 = 100;
type Key = u64;

#[derive(Debug, serde::Deserialize)]
struct AccessRecord {
    timestamp: u64,
    command: u8,
    key: u64,
    size: u32,
    ttl: u32,
}

struct SimulationResult {
    points: Vec<(f64, f64)>,
    label: String,
}

// Use multi thread to simulate
fn simulation<P: EvictPolicy>(
    access_records: Arc<Vec<AccessRecord>>,
    mut sim: MiniSim<P>,
    label: String,
) -> SimulationResult {
    let start = std::time::Instant::now();
    for access in access_records.iter() {
        sim.handle(access);
    }
    let points = sim.curve();
    let elapsed = start.elapsed();
    info!("{label} simulation took {elapsed:?}");
    SimulationResult { points, label }
}

// Draw the lines
// Parameter: Vec<SimulationResult>
fn draw_lines(results: &[SimulationResult], path: &str) {
    let mut fg = Figure::new();

    let width = 1920;
    let height = 1080;

    fg.set_title("Miss ratio curve");
    let axes = fg.axes2d();
    for result in results {
        axes.set_x_label("Cache size", &[])
            .set_y_label("Miss ratio", &[])
            .lines(
                result.points.iter().map(|(x, _)| *x),
                result.points.iter().map(|(_, y)| *y),
                &[Caption(result.label.as_str())],
            );
    }
    fg.save_to_png(path, width, height).unwrap();
}

// Simulate for a access reocrds
// Use multi thread to simulate
// 1. simulate without shards
// 2. simulate with 10% shards
// 3. simulate with 1% shards
// collect result to draw
fn simulate_all<P: EvictPolicy + 'static>(
    access_records: Arc<Vec<AccessRecord>>,
    max_cache_size: u64,
    path: &str,
) {
    let sim_without_shards = MiniSim::<P>::new(max_cache_size, None);
    let sim_10_shards = MiniSim::new(max_cache_size, Some(Box::new(ShardsFixedRate::new(10))));
    let sim_1_shards = MiniSim::new(max_cache_size, Some(Box::new(ShardsFixedRate::new(1))));

    let simulations = vec![
        ("Without shards", sim_without_shards),
        ("10% shards", sim_10_shards),
        ("1% shards", sim_1_shards),
    ];

    let handles: Vec<_> = simulations
        .into_iter()
        .map(|(label, sim)| {
            let access_records = Arc::clone(&access_records);
            let label = label.to_string();
            thread::spawn(move || simulation(access_records, sim, label))
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();

    draw_lines(&results, path);
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

    debug!("Access records: length: {}", access_records.len());
    debug!("First access record: {:?}", access_records[0]);

    // 启动两个线程，一个是sim，一个是sim_without_sim
    let access_records = Arc::new(access_records);
    simulate_all::<LruPolicy>(
        access_records.clone(),
        4000000,
        "./lru_miss_ratio_curve.png",
    );
    simulate_all::<LruPolicy>(access_records, 4000000, "./fifo_miss_ratio_curve.png");
    debug!("Simulation completed successfully");

    Ok(())
}
