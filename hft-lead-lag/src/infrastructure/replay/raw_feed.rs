use crate::application::services::{LeadLagStrategy, LeadLagStrategyConfig, SignalDirection};
use crate::domain::ExchangeId;
use crate::infrastructure::exchanges::{BinanceMarketData, GateMarketData};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RawFeedExchange {
    Binance,
    Gate,
}

impl RawFeedExchange {
    fn to_domain_exchange(self) -> ExchangeId {
        match self {
            Self::Binance => ExchangeId::BinanceFutures,
            Self::Gate => ExchangeId::GateFutures,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFeedFrame {
    pub seq: u64,
    pub exchange: RawFeedExchange,
    pub recv_ts_ns: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawFeedFrameLine {
    seq: u64,
    exchange: RawFeedExchange,
    recv_ts_ns: i64,
    payload_b64: String,
}

pub const RAW_FEED_RECORD_PATH_ENV: &str = "RAW_FEED_RECORD_PATH";

struct RawFeedRecorderState {
    next_seq: u64,
    writer: std::io::BufWriter<std::fs::File>,
}

pub struct RawFeedRecorder {
    state: Mutex<RawFeedRecorderState>,
}

impl RawFeedRecorder {
    pub fn spawn(path: impl AsRef<Path>) -> io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            state: Mutex::new(RawFeedRecorderState {
                next_seq: 0,
                writer: std::io::BufWriter::new(file),
            }),
        })
    }

    pub fn record(
        &self,
        exchange: RawFeedExchange,
        recv_ts_ns: i64,
        payload: &[u8],
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("raw-feed recorder mutex poisoned"))?;
        let seq = state.next_seq;
        let line = RawFeedFrameLine {
            seq,
            exchange,
            recv_ts_ns,
            payload_b64: STANDARD.encode(payload),
        };
        serde_json::to_writer(&mut state.writer, &line)?;
        state.writer.write_all(b"\n")?;
        state.writer.flush()?;
        state.next_seq = state.next_seq.saturating_add(1);
        Ok(())
    }
}

pub fn raw_feed_recorder_from_env() -> io::Result<Option<Arc<RawFeedRecorder>>> {
    let Some(raw) = std::env::var(RAW_FEED_RECORD_PATH_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };
    let recorder = RawFeedRecorder::spawn(raw)?;
    Ok(Some(Arc::new(recorder)))
}

pub struct RawFeedReplayReader {}

impl RawFeedReplayReader {
    pub fn read_all(path: impl AsRef<Path>) -> io::Result<Vec<RawFeedFrame>> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut out = Vec::new();
        let mut expected_seq = 0u64;

        for (line_no, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let frame: RawFeedFrameLine = serde_json::from_str(&line).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("line {} invalid json: {err}", line_no + 1),
                )
            })?;
            if frame.seq != expected_seq {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "line {} invalid seq: expected {}, got {}",
                        line_no + 1,
                        expected_seq,
                        frame.seq
                    ),
                ));
            }
            let payload = STANDARD.decode(frame.payload_b64).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("line {} invalid payload_b64: {err}", line_no + 1),
                )
            })?;
            out.push(RawFeedFrame {
                seq: frame.seq,
                exchange: frame.exchange,
                recv_ts_ns: frame.recv_ts_ns,
                payload,
            });
            expected_seq = expected_seq.saturating_add(1);
        }

        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySignalEvent {
    pub frame_seq: u64,
    pub symbol: String,
    pub direction: &'static str,
    pub spread_bps_scaled: i64,
    pub bid_ask_bps_scaled: i64,
    pub ask_bid_bps_scaled: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplaySignalTrace {
    pub parsed_ticker_count: usize,
    pub signals: Vec<ReplaySignalEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFeedReplayRouting {
    pub primary: RawFeedExchange,
    pub hedge: RawFeedExchange,
}

impl Default for RawFeedReplayRouting {
    fn default() -> Self {
        Self {
            primary: RawFeedExchange::Binance,
            hedge: RawFeedExchange::Gate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayDeterminismReport {
    pub deterministic: bool,
    pub parsed_ticker_count: usize,
    pub signal_count: usize,
    pub mismatch_index: Option<usize>,
}

fn scale_bps(value: f64) -> i64 {
    if value.is_nan() {
        i64::MIN
    } else {
        (value * 1_000_000.0).round() as i64
    }
}

pub fn replay_signal_trace(
    frames: &[RawFeedFrame],
    strategy_symbols: &[String],
    routing: RawFeedReplayRouting,
) -> io::Result<ReplaySignalTrace> {
    if routing.primary == routing.hedge {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "primary and hedge exchange must differ",
        ));
    }

    let mut binance = BinanceMarketData::new();
    let mut gate = GateMarketData::new();
    binance
        .set_strategy_symbol_ids(strategy_symbols)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
    gate.set_strategy_symbol_ids(strategy_symbols)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;

    let mut strategy_cfg = LeadLagStrategyConfig {
        symbols: strategy_symbols.to_vec(),
        ..Default::default()
    };
    strategy_cfg.primary_exchange = routing.primary.to_domain_exchange();
    strategy_cfg.hedge_exchange = routing.hedge.to_domain_exchange();
    let mut strategy = LeadLagStrategy::new(strategy_cfg);

    let mut trace = ReplaySignalTrace::default();

    for frame in frames {
        let maybe_ticker = match frame.exchange {
            RawFeedExchange::Binance => {
                binance.parse_book_ticker_for_replay(&frame.payload, frame.recv_ts_ns)
            }
            RawFeedExchange::Gate => {
                gate.parse_book_ticker_for_replay(&frame.payload, frame.recv_ts_ns)
            }
        };
        let Some(ticker) = maybe_ticker else {
            continue;
        };

        trace.parsed_ticker_count = trace.parsed_ticker_count.saturating_add(1);
        let symbol = std::str::from_utf8(ticker.symbol.as_ref())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if frame.exchange == routing.primary {
            strategy.update_primary_book(ticker);
        } else {
            strategy.update_hedge_book(ticker);
        }

        if symbol.is_empty() {
            continue;
        }
        if let Some(signal) = strategy.check_signal(&symbol, frame.recv_ts_ns) {
            let direction = match signal.direction {
                SignalDirection::LongLagger => "LONG_LAGGER",
                SignalDirection::ShortLagger => "SHORT_LAGGER",
            };
            trace.signals.push(ReplaySignalEvent {
                frame_seq: frame.seq,
                symbol: signal.symbol,
                direction,
                spread_bps_scaled: scale_bps(signal.spread_bps),
                bid_ask_bps_scaled: scale_bps(signal.bid_ask_bps),
                ask_bid_bps_scaled: scale_bps(signal.ask_bid_bps),
            });
        }
    }

    Ok(trace)
}

pub fn replay_signal_trace_from_file(
    path: impl AsRef<Path>,
    strategy_symbols: &[String],
    routing: RawFeedReplayRouting,
) -> io::Result<ReplaySignalTrace> {
    let frames = RawFeedReplayReader::read_all(path)?;
    replay_signal_trace(&frames, strategy_symbols, routing)
}

pub fn verify_signal_replay_determinism(
    frames: &[RawFeedFrame],
    strategy_symbols: &[String],
    routing: RawFeedReplayRouting,
) -> io::Result<ReplayDeterminismReport> {
    let first = replay_signal_trace(frames, strategy_symbols, routing)?;
    let second = replay_signal_trace(frames, strategy_symbols, routing)?;

    let mismatch_index = first
        .signals
        .iter()
        .zip(second.signals.iter())
        .position(|(left, right)| left != right)
        .or_else(|| {
            if first.signals.len() != second.signals.len() {
                Some(first.signals.len().min(second.signals.len()))
            } else {
                None
            }
        });
    let deterministic = first == second;

    Ok(ReplayDeterminismReport {
        deterministic,
        parsed_ticker_count: first.parsed_ticker_count,
        signal_count: first.signals.len(),
        mismatch_index,
    })
}

pub fn verify_signal_replay_determinism_from_file(
    path: impl AsRef<Path>,
    strategy_symbols: &[String],
    routing: RawFeedReplayRouting,
) -> io::Result<ReplayDeterminismReport> {
    let frames = RawFeedReplayReader::read_all(path)?;
    verify_signal_replay_determinism(&frames, strategy_symbols, routing)
}

#[cfg(test)]
mod tests {
    use super::{
        raw_feed_recorder_from_env, replay_signal_trace, verify_signal_replay_determinism,
        RawFeedExchange, RawFeedRecorder, RawFeedReplayReader, RawFeedReplayRouting,
        RAW_FEED_RECORD_PATH_ENV,
    };
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file(prefix: &str) -> PathBuf {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("hft-{prefix}-{now_ns}.jsonl"))
    }

    fn env_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock")
    }

    #[test]
    fn record_and_read_round_trip_keeps_order_and_payload() {
        let path = temp_file("raw-feed-roundtrip");
        let recorder = RawFeedRecorder::spawn(&path).expect("spawn recorder");
        recorder
            .record(RawFeedExchange::Binance, 1_000, br#"{"e":"bookTicker"}"#)
            .expect("record binance");
        recorder
            .record(RawFeedExchange::Gate, 1_100, b"\x01\x02\x03\x04")
            .expect("record gate");
        drop(recorder);

        let frames = RawFeedReplayReader::read_all(&path).expect("read replay");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].seq, 0);
        assert_eq!(frames[0].exchange, RawFeedExchange::Binance);
        assert_eq!(frames[0].recv_ts_ns, 1_000);
        assert_eq!(frames[0].payload, br#"{"e":"bookTicker"}"#);
        assert_eq!(frames[1].seq, 1);
        assert_eq!(frames[1].exchange, RawFeedExchange::Gate);
        assert_eq!(frames[1].recv_ts_ns, 1_100);
        assert_eq!(frames[1].payload, b"\x01\x02\x03\x04");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_reader_rejects_invalid_payload_encoding() {
        let path = temp_file("raw-feed-invalid");
        let mut file = std::fs::File::create(&path).expect("create file");
        writeln!(
            file,
            r#"{{"seq":0,"exchange":"binance","recv_ts_ns":123,"payload_b64":"!!!"}}"#
        )
        .expect("write invalid line");
        drop(file);

        let err = RawFeedReplayReader::read_all(&path).expect_err("invalid payload must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_reader_rejects_invalid_json_line() {
        let path = temp_file("raw-feed-invalid-json");
        let mut file = std::fs::File::create(&path).expect("create file");
        writeln!(file, r#"{{"seq":0,"exchange":"binance","recv_ts_ns":123"#).expect("write line");
        drop(file);

        let err = RawFeedReplayReader::read_all(&path).expect_err("invalid json must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("invalid json"),
            "error must include json parsing context"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_reader_rejects_out_of_order_sequence() {
        let path = temp_file("raw-feed-invalid-seq");
        let mut file = std::fs::File::create(&path).expect("create file");
        writeln!(
            file,
            r#"{{"seq":0,"exchange":"binance","recv_ts_ns":123,"payload_b64":"AA=="}}"#
        )
        .expect("write seq0");
        writeln!(
            file,
            r#"{{"seq":2,"exchange":"gate","recv_ts_ns":124,"payload_b64":"AQ=="}}"#
        )
        .expect("write seq2");
        drop(file);

        let err = RawFeedReplayReader::read_all(&path).expect_err("invalid seq must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("invalid seq"),
            "error must include seq validation context"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recorder_from_env_returns_none_when_unset() {
        let _lock = env_test_lock();
        std::env::remove_var(RAW_FEED_RECORD_PATH_ENV);
        let recorder = raw_feed_recorder_from_env().expect("from env");
        assert!(recorder.is_none());
    }

    #[test]
    fn replay_signal_trace_is_deterministic_for_same_input() {
        let path = temp_file("raw-feed-deterministic");
        let recorder = RawFeedRecorder::spawn(&path).expect("spawn recorder");
        recorder
            .record(
                RawFeedExchange::Binance,
                1_000_000_000,
                br#"{"e":"bookTicker","s":"BTCUSDT","b":"110.0","B":"1.0","a":"111.0","A":"1.0","T":1700000000000}"#,
            )
            .expect("record binance");
        recorder
            .record(
                RawFeedExchange::Gate,
                1_000_000_100,
                br#"{"channel":"futures.book_ticker","event":"update","contract":"BTC_USDT","b":"100.0","B":"1.0","a":"101.0","A":"1.0","t":1700000000000}"#,
            )
            .expect("record gate");
        drop(recorder);

        let frames = RawFeedReplayReader::read_all(&path).expect("read replay");
        let symbols = vec!["BTCUSDT".to_string()];
        let trace =
            replay_signal_trace(&frames, &symbols, RawFeedReplayRouting::default()).expect("trace");
        assert!(trace.parsed_ticker_count >= 2);
        assert!(!trace.signals.is_empty(), "expected at least one signal");

        let report =
            verify_signal_replay_determinism(&frames, &symbols, RawFeedReplayRouting::default())
                .expect("determinism");
        assert!(report.deterministic);
        assert!(report.signal_count >= 1);
        assert_eq!(report.mismatch_index, None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn concurrent_recording_keeps_monotonic_sequence() {
        let path = temp_file("raw-feed-concurrent");
        let recorder = Arc::new(RawFeedRecorder::spawn(&path).expect("spawn recorder"));
        let mut joins = Vec::new();
        for thread_id in 0..6usize {
            let recorder = recorder.clone();
            joins.push(std::thread::spawn(move || {
                for idx in 0..120usize {
                    let payload = format!(r#"{{"tid":{thread_id},"idx":{idx}}}"#).into_bytes();
                    recorder
                        .record(
                            if thread_id % 2 == 0 {
                                RawFeedExchange::Binance
                            } else {
                                RawFeedExchange::Gate
                            },
                            1_000_000 + idx as i64,
                            &payload,
                        )
                        .expect("concurrent record");
                }
            }));
        }
        for join in joins {
            join.join().expect("thread join");
        }
        drop(recorder);

        let frames = RawFeedReplayReader::read_all(&path).expect("read replay");
        assert_eq!(frames.len(), 6 * 120);
        assert_eq!(frames.first().map(|f| f.seq), Some(0));
        assert_eq!(frames.last().map(|f| f.seq), Some((6 * 120 - 1) as u64));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    #[ignore = "profiling-only replay benchmark harness"]
    fn bench_replay_signal_trace_profile() {
        let symbols = vec!["BTCUSDT".to_string()];
        let mut frames = Vec::with_capacity(50_000);
        for i in 0..25_000u64 {
            frames.push(super::RawFeedFrame {
                seq: i * 2,
                exchange: RawFeedExchange::Binance,
                recv_ts_ns: 1_000_000_000 + (i as i64) * 100,
                payload: br#"{"e":"bookTicker","s":"BTCUSDT","b":"110.0","B":"1.0","a":"111.0","A":"1.0","T":1700000000000}"#.to_vec(),
            });
            frames.push(super::RawFeedFrame {
                seq: i * 2 + 1,
                exchange: RawFeedExchange::Gate,
                recv_ts_ns: 1_000_000_050 + (i as i64) * 100,
                payload: br#"{"channel":"futures.book_ticker","event":"update","contract":"BTC_USDT","b":"100.0","B":"1.0","a":"101.0","A":"1.0","t":1700000000000}"#.to_vec(),
            });
        }
        let start = std::time::Instant::now();
        let trace =
            replay_signal_trace(&frames, &symbols, RawFeedReplayRouting::default()).expect("trace");
        let elapsed = start.elapsed();
        let ns_per_frame = elapsed.as_nanos() / frames.len() as u128;
        eprintln!(
            "bench_replay_signal_trace_profile: frames={} parsed={} signals={} elapsed_ms={} ns_per_frame={}",
            frames.len(),
            trace.parsed_ticker_count,
            trace.signals.len(),
            elapsed.as_millis(),
            ns_per_frame
        );
        assert!(trace.parsed_ticker_count > 0);
    }
}
