use super::{FileFingerprint, TraderConfig, hash_content_deterministic};
use serde::Deserialize;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub(super) struct GridAxisF64 {
    min: f64,
    max: f64,
    step: f64,
}

impl Default for GridAxisF64 {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            step: 1.0,
        }
    }
}

impl GridAxisF64 {
    fn values(&self, name: &str) -> Result<Vec<f64>, String> {
        if !self.min.is_finite() || !self.max.is_finite() || !self.step.is_finite() {
            return Err(format!("{name}: min/max/step must be finite"));
        }
        if self.step <= 0.0 {
            return Err(format!("{name}: step must be > 0"));
        }
        if self.max < self.min {
            return Err(format!("{name}: max must be >= min"));
        }

        let mut values = Vec::new();
        let mut current = self.min;
        let mut guard = 0usize;
        while current <= self.max + self.step * 1e-9 {
            values.push((current * 1_000_000.0).round() / 1_000_000.0);
            current += self.step;
            guard += 1;
            if guard > 10_000 {
                return Err(format!("{name}: generated too many points (>10000)"));
            }
        }
        if values.is_empty() {
            values.push(self.min);
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub(super) struct GridAxisI64 {
    min: i64,
    max: i64,
    step: i64,
}

impl Default for GridAxisI64 {
    fn default() -> Self {
        Self {
            min: 0,
            max: 0,
            step: 1,
        }
    }
}

impl GridAxisI64 {
    fn values(&self, name: &str) -> Result<Vec<i64>, String> {
        if self.step <= 0 {
            return Err(format!("{name}: step must be > 0"));
        }
        if self.max < self.min {
            return Err(format!("{name}: max must be >= min"));
        }

        let mut values = Vec::new();
        let mut current = self.min;
        let mut guard = 0usize;
        while current <= self.max {
            values.push(current);
            current = current.saturating_add(self.step);
            guard += 1;
            if guard > 10_000 {
                return Err(format!("{name}: generated too many points (>10000)"));
            }
        }
        if values.is_empty() {
            values.push(self.min);
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub(super) struct RuntimeGridConfig {
    pub(super) enabled: bool,
    pub(super) watch_interval_ms: u64,
    pub(super) apply_interval_ms: u64,
    pub(super) max_configs: usize,
    pub(super) gap_threshold_bps: GridAxisF64,
    pub(super) target_ratio: GridAxisF64,
    pub(super) stop_loss_bps: GridAxisF64,
    pub(super) max_hold_ms: GridAxisI64,
    pub(super) max_spread_bps: GridAxisF64,
    pub(super) trailing_decay_ratio: GridAxisF64,
    pub(super) baseline_window_ms: GridAxisI64,
}

impl Default for RuntimeGridConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            watch_interval_ms: 5_000,
            apply_interval_ms: 5 * 60 * 1_000,
            max_configs: 1_500,
            gap_threshold_bps: GridAxisF64 {
                min: 30.0,
                max: 80.0,
                step: 10.0,
            },
            target_ratio: GridAxisF64 {
                min: 0.3,
                max: 0.7,
                step: 0.1,
            },
            stop_loss_bps: GridAxisF64 {
                min: 8.0,
                max: 40.0,
                step: 4.0,
            },
            max_hold_ms: GridAxisI64 {
                min: 5_000,
                max: 30_000,
                step: 5_000,
            },
            max_spread_bps: GridAxisF64 {
                min: 3.0,
                max: 5.0,
                step: 1.0,
            },
            trailing_decay_ratio: GridAxisF64 {
                min: 0.3,
                max: 0.7,
                step: 0.1,
            },
            baseline_window_ms: GridAxisI64 {
                min: 10_000,
                max: 60_000,
                step: 10_000,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeGridGeneration {
    pub(super) config: RuntimeGridConfig,
    pub(super) configs: Vec<TraderConfig>,
    pub(super) signature: u64,
    pub(super) modified: FileFingerprint,
}

const DEFAULT_RUNTIME_GRID_CONFIG_TOML: &str = r#"# Runtime grid hot-reload (deal-hunt phase A)
enabled = true
watch_interval_ms = 5000
apply_interval_ms = 300000
max_configs = 1500

[gap_threshold_bps]
min = 30.0
max = 80.0
step = 10.0

[target_ratio]
min = 0.3
max = 0.7
step = 0.1

[stop_loss_bps]
min = 8.0
max = 40.0
step = 4.0

[max_hold_ms]
min = 5000
max = 30000
step = 5000

[max_spread_bps]
min = 3.0
max = 5.0
step = 1.0

[trailing_decay_ratio]
min = 0.3
max = 0.7
step = 0.1

[baseline_window_ms]
min = 10000
max = 60000
step = 10000
"#;

pub(super) fn ensure_runtime_grid_config_file(
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, DEFAULT_RUNTIME_GRID_CONFIG_TOML)?;
    info!("Created default runtime grid config: {}", path.display());
    Ok(())
}

fn runtime_grid_signature(configs: &[TraderConfig]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    configs.len().hash(&mut hasher);
    for cfg in configs {
        cfg.config_id().hash(&mut hasher);
    }
    hasher.finish()
}

fn downsample_configs(configs: Vec<TraderConfig>, limit: usize) -> Vec<TraderConfig> {
    if limit == 0 || configs.is_empty() || configs.len() <= limit {
        return configs;
    }
    let stride = configs.len() as f64 / limit as f64;
    let mut selected = Vec::with_capacity(limit);
    let mut seen = HashSet::with_capacity(limit * 2);
    for i in 0..limit {
        let idx = ((i as f64) * stride).floor() as usize;
        let cfg = configs[idx.min(configs.len() - 1)];
        if seen.insert(cfg.config_id()) {
            selected.push(cfg);
        }
    }
    if selected.len() < limit {
        for cfg in configs {
            if seen.insert(cfg.config_id()) {
                selected.push(cfg);
                if selected.len() == limit {
                    break;
                }
            }
        }
    }
    selected
}

fn build_runtime_grid(cfg: &RuntimeGridConfig) -> Result<Vec<TraderConfig>, String> {
    if cfg.max_configs == 0 {
        return Err("max_configs must be > 0".to_string());
    }
    let gaps = cfg.gap_threshold_bps.values("gap_threshold_bps")?;
    let targets = cfg.target_ratio.values("target_ratio")?;
    let stops = cfg.stop_loss_bps.values("stop_loss_bps")?;
    let holds = cfg.max_hold_ms.values("max_hold_ms")?;
    let spreads = cfg.max_spread_bps.values("max_spread_bps")?;
    let trails = cfg.trailing_decay_ratio.values("trailing_decay_ratio")?;
    let baselines = cfg.baseline_window_ms.values("baseline_window_ms")?;

    let total = gaps.len()
        * targets.len()
        * stops.len()
        * holds.len()
        * spreads.len()
        * trails.len()
        * baselines.len();
    if total == 0 {
        return Err("runtime grid produced zero combinations".to_string());
    }

    let mut configs = Vec::with_capacity(total.min(cfg.max_configs));
    let base = TraderConfig::default();
    for &gap in &gaps {
        for &target in &targets {
            for &stop in &stops {
                for &hold in &holds {
                    for &spread in &spreads {
                        for &trail in &trails {
                            for &baseline in &baselines {
                                configs.push(TraderConfig {
                                    spike_threshold_bps: gap,
                                    target_ratio: target,
                                    stop_loss_bps: stop,
                                    max_hold_ms: hold,
                                    max_spread_bps: spread,
                                    trailing_decay_ratio: trail,
                                    baseline_window_ms: baseline,
                                    ..base
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    let mut deduped = Vec::with_capacity(configs.len());
    let mut ids = HashSet::with_capacity(configs.len());
    for cfg in configs {
        if ids.insert(cfg.config_id()) {
            deduped.push(cfg);
        }
    }
    Ok(downsample_configs(deduped, cfg.max_configs))
}

fn load_runtime_grid_generation(path: &Path) -> Result<RuntimeGridGeneration, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read runtime grid {}: {e}", path.display()))?;
    let config: RuntimeGridConfig = toml::from_str(&content)
        .map_err(|e| format!("parse runtime grid {}: {e}", path.display()))?;
    let metadata = std::fs::metadata(path)
        .map_err(|e| format!("runtime grid metadata {}: {e}", path.display()))?;
    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let modified = FileFingerprint {
        modified,
        len: metadata.len(),
        content_hash: hash_content_deterministic(content.as_bytes()),
    };
    let configs = if config.enabled {
        build_runtime_grid(&config)?
    } else {
        Vec::new()
    };
    let signature = runtime_grid_signature(&configs);
    Ok(RuntimeGridGeneration {
        config,
        configs,
        signature,
        modified,
    })
}

pub(super) async fn load_runtime_grid_generation_async(
    path: PathBuf,
) -> Result<RuntimeGridGeneration, String> {
    tokio::task::spawn_blocking(move || load_runtime_grid_generation(&path))
        .await
        .map_err(|e| format!("runtime-grid task join error: {e}"))?
}
