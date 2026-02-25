# Duplication, Redundancy & Complexity Review (R4)

## Findings
- **P3** `src/main.rs:1031-1093`  
  `maybe_handle_trial_batch_file` and `maybe_handle_trial_batch_queue` duplicate load/apply/ack/error flow with minor IO differences.

- **P3** `src/main.rs:472-617`  
  Trial-batch parsing, fingerprinting, ack serialization, and queue sorting helpers remain embedded in `main.rs` and repeated across adjacent flows.

- **P2** `src/infrastructure/db.rs:103-141`, `src/infrastructure/db.rs:894-905`  
  Schema complexity includes currently unused family/portfolio tables, adding migration and maintenance overhead.

## Simplification Targets
- Consolidate trial-batch apply+ack path into shared helper/service.
- Move trial-batch domain helpers out of `main.rs` into focused module.
- Either wire family/portfolio schema into active flow or remove for YAGNI.
