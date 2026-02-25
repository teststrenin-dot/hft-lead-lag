# Bugs & Errors Review (R3)

## Findings
- **P1** `src/infrastructure/db.rs:368-441`  
  `DbWriter::send` creates deferred async/task senders whenever both primary and overflow are full. No bound exists for number of deferred senders/queued commands; sustained bursts can exhaust memory/threads.

- **P2** `src/main.rs:542-575`  
  `list_trial_batch_queue_files` sorts all timestamped filenames before non-timestamped; a persistent stream of timestamped files can indefinitely postpone manual files.

- **P2** `src/domain/screener/mod.rs:385,444`  
  Symbol map only grows (`entry(...).or_default()`), while each screener request rebuilds/sorts full row list. This is a performance-bug vector on long-running sessions.

## Repro/Test Gaps
- Missing stress test for bounded deferred backlog in DB writer.
- Missing starvation test for mixed queue filenames (timestamped + non-timestamped).
