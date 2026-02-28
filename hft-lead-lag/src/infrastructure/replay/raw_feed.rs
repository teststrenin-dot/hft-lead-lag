use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RawFeedExchange {
    Binance,
    Gate,
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

pub struct RawFeedRecorder {
    seq: AtomicU64,
    writer: Mutex<std::io::BufWriter<std::fs::File>>,
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
            seq: AtomicU64::new(0),
            writer: Mutex::new(std::io::BufWriter::new(file)),
        })
    }

    pub fn record(&self, exchange: RawFeedExchange, recv_ts_ns: i64, payload: &[u8]) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let line = RawFeedFrameLine {
            seq,
            exchange,
            recv_ts_ns,
            payload_b64: STANDARD.encode(payload),
        };
        let Ok(mut writer) = self.writer.lock() else {
            return;
        };
        if serde_json::to_writer(&mut *writer, &line).is_err() {
            return;
        }
        let _ = writer.write_all(b"\n");
        let _ = writer.flush();
    }
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

#[cfg(test)]
mod tests {
    use super::{RawFeedExchange, RawFeedRecorder, RawFeedReplayReader};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file(prefix: &str) -> PathBuf {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("hft-{prefix}-{now_ns}.jsonl"))
    }

    #[test]
    fn record_and_read_round_trip_keeps_order_and_payload() {
        let path = temp_file("raw-feed-roundtrip");
        let recorder = RawFeedRecorder::spawn(&path).expect("spawn recorder");
        recorder.record(RawFeedExchange::Binance, 1_000, br#"{"e":"bookTicker"}"#);
        recorder.record(RawFeedExchange::Gate, 1_100, b"\x01\x02\x03\x04");
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
}
