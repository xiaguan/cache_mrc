use std::{
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
};

use crate::AccessRecord;
use clap::Parser;
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[clap(author, version, about, long_about = None)]
#[serde(default)]
pub struct Config {
    /// Path to the configuration file
    #[arg(long, value_name = "FILE")]
    #[serde(skip)]
    pub config_file: Option<PathBuf>,

    /// Path to the trace file
    #[arg(long, value_name = "FILE")]
    pub trace: Option<PathBuf>,

    /// Sample rate
    #[arg(long)]
    pub sample_rate: Option<f64>,

    /// Path to the output file
    #[arg(long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Cache eviction policies (LRU, FIFO, etc.)
    #[arg(long, value_enum, use_value_delimiter = true, value_delimiter = ',')]
    #[serde(default = "default_eviction_policies")]
    pub policies: Option<Vec<EvictionPolicy>>,

    /// Cache size (e.g., 100KB, 2MB)
    #[arg(short, long, value_parser = parse_size)]
    #[serde(deserialize_with = "deserialize_cache_size")]
    pub cache_size: Option<u64>,

    #[arg(long)]
    pub timestamp: Option<i32>,

    #[arg(long)]
    pub command: Option<i32>,

    #[arg(long)]
    pub key: Option<i32>,

    #[arg(long)]
    pub size: Option<i32>,

    #[arg(long)]
    pub ttl: Option<i32>,
}

#[derive(Debug)]
pub struct InnerConfig {
    pub output: PathBuf,
    pub policies: Vec<EvictionPolicy>,
    pub cache_size: u64,
    pub sample_rate: Option<f64>,
}

impl From<Config> for InnerConfig {
    fn from(config: Config) -> Self {
        InnerConfig {
            output: config.output.unwrap(),
            policies: config.policies.unwrap(),
            cache_size: config.cache_size.unwrap(),
            sample_rate: config.sample_rate,
        }
    }
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let mut args: Config = toml::from_str(&content)?;
        args.config_file = Some(path.clone());
        Ok(args)
    }
}

fn default_eviction_policies() -> Option<Vec<EvictionPolicy>> {
    Some(vec![EvictionPolicy::LRU])
}

fn deserialize_cache_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    parse_size(&s).map_err(serde::de::Error::custom)
}

// 确保 EvictionPolicy 可以被序列化和反序列化
#[derive(clap::ValueEnum, Clone, Debug, Deserialize, Serialize)]
pub enum EvictionPolicy {
    LRU,
    FIFO,
    LFU,
    TWOQ,
}

impl EvictionPolicy {
    pub fn to_string(&self) -> String {
        match self {
            EvictionPolicy::LRU => "LRU",
            EvictionPolicy::FIFO => "FIFO",
            EvictionPolicy::LFU => "LFU",
            EvictionPolicy::TWOQ => "TWOQ",
        }
        .to_string()
    }
}

fn parse_size(s: &str) -> Result<Option<u64>, String> {
    let s = s.trim().to_uppercase();
    let cache_size = if s.ends_with("KB") {
        s[..s.len() - 2]
            .parse::<u64>()
            .map(|n| n * 1024)
            .map_err(|e| e.to_string())
    } else if s.ends_with("MB") {
        s[..s.len() - 2]
            .parse::<u64>()
            .map(|n| n * 1024 * 1024)
            .map_err(|e| e.to_string())
    } else if s.ends_with("GB") {
        s[..s.len() - 2]
            .parse::<u64>()
            .map(|n| n * 1024 * 1024 * 1024)
            .map_err(|e| e.to_string())
    } else {
        s.parse::<u64>().map_err(|e| e.to_string())
    };
    cache_size.map(Some)
}

pub fn load_access_records(arg: &Config) -> Vec<AccessRecord> {
    let trace_path = arg.trace.as_ref().unwrap();
    let file = File::open(trace_path).unwrap();
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(reader);

    if is_default_parsing(arg) {
        parse_default(&mut rdr)
    } else {
        parse_custom(arg, &mut rdr)
    }
}

fn is_default_parsing(arg: &Config) -> bool {
    arg.timestamp.is_none()
        && arg.command.is_none()
        && arg.key.is_none()
        && arg.size.is_none()
        && arg.ttl.is_none()
}

fn parse_default(rdr: &mut csv::Reader<BufReader<File>>) -> Vec<AccessRecord> {
    debug!("Parsing access records with default fields");
    let mut access_records = Vec::new();
    for result in rdr.deserialize() {
        let record: AccessRecord = result.unwrap();
        access_records.push(record);
    }
    access_records
}

fn parse_custom(arg: &Config, rdr: &mut csv::Reader<BufReader<File>>) -> Vec<AccessRecord> {
    let mut access_records = Vec::new();
    for result in rdr.records() {
        let record = result.unwrap();
        let timestamp = parse_field(&record, arg.timestamp, 0);
        let command = parse_field(&record, arg.command, 0) as u8;
        let key = parse_field(&record, arg.key, 0);
        let size = parse_field(&record, arg.size, 1) as u32;
        let ttl = parse_field(&record, arg.ttl, 0) as u32;

        access_records.push(AccessRecord {
            timestamp,
            command,
            key,
            size,
            ttl,
        });
    }
    access_records
}

fn parse_field(record: &csv::StringRecord, field_opt: Option<i32>, default: u64) -> u64 {
    if let Some(index) = field_opt {
        if index == -1 {
            default
        } else {
            record[index as usize].parse().unwrap()
        }
    } else {
        default
    }
}

impl Config {
    pub fn load() -> Self {
        let args = Config::parse();
        if let Some(path) = &args.config_file {
            match Config::from_file(path) {
                Ok(config) => config,
                Err(e) => {
                    error!("Failed to load configuration file: {}", e);
                    args
                }
            }
        } else {
            args
        }
    }
}
