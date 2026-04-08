# BDA
# AIS CSV Processing

This project processes AIS CSV files and produces:

- `matched_pairs.csv` for loitering vessel pairs
- `anomaly_list.csv` for vessel-level anomaly summaries
- `memory_usage_per_worker.png` for RAM usage during the run

The main script is [process_ais_csv.py](/Users/ziviletareilyte/Documents/naujas_test/Pagrindinis/2026-pirma-uzduotis/process_ais_csv.py).

## What The Script Detects

The pipeline analyzes AIS records and looks for these behaviors:

- `Loitering`: two slow vessels remain within 500 meters for at least 2 hours
- `Going dark`: a vessel disappears for more than 4 hours and reappears over 500 meters away
- `Draught change`: draught changes by more than 5% after a gap longer than 2 hours
- `Impossible speed`: inferred speed between consecutive valid points exceeds 60 knots

The anomaly summary also computes a `dfsi` score:

```text
dfsi = (max_gap_hours / 2.0) + (total_impossible_distance_nm / 10.0) + (draught_change_count * 15.0)
```

## Input Rules

Before rows are processed, the script applies broad validation rules. A row is skipped if:

- MMSI is empty, malformed, repeated digits only, or in a known invalid set
- `Type of mobile` is not `Class A` or `Class B`
- `Navigational status` is `Moored` or `At anchor`

Loitering detection then applies extra pair-specific rules:

- `SOG` must be below `1.0`
- latitude and longitude must be valid

## Input Format

The script expects AIS CSV files with columns matching the Danish AIS export layout used in this folder, including:

- `# Timestamp`
- `Type of mobile`
- `MMSI`
- `Latitude`
- `Longitude`
- `Navigational status`
- `SOG`
- `Draught`

Example files in this project:

- [aisdk-2025-05-02.csv](/Users/ziviletareilyte/Documents/naujas_test/Pagrindinis/2026-pirma-uzduotis/aisdk-2025-05-02.csv)
- [aisdk-2025-05-03.csv](/Users/ziviletareilyte/Documents/naujas_test/Pagrindinis/2026-pirma-uzduotis/aisdk-2025-05-03.csv)

## Requirements

Use Python 3 and install these libraries:

```bash
pip install matplotlib psutil
```

## How To Run

From the project directory:

```bash
python3 process_ais_csv.py \
  --input-file aisdk-2025-05-02.csv \
  --input-file aisdk-2025-05-03.csv
```

You can pass one or more `--input-file` arguments.

## Output Files

### `matched_pairs.csv`

Contains vessel pairs that satisfied the loitering rule.

Columns:

- `mmsi_a`
- `mmsi_b`
- `start`
- `end`
- `latest_latitude`
- `latest_longitude`

### `anomaly_list.csv`

Contains one summary row per MMSI.

Columns:

- `mmsi`
- `going_dark_anomaly`
- `draught_change`
- `draught_change_count`
- `impossible_speed`
- `loitering`
- `loitering_count`
- `max_gap_hours`
- `total_impossible_distance_nm`
- `latest_anomaly_latitude`
- `latest_anomaly_longitude`
- `dfsi`

### `memory_usage_per_worker.png`

Shows memory usage over time for:

- main process
- pair worker
- anomaly worker

## Runtime Behavior

The script uses multiprocessing:

- one worker for pair and loitering detection
- one worker for anomaly detection
- the main process for CSV streaming, batching, progress logging, and memory logging

Accepted rows are batched and sent to both workers.

## Notes

- Loitering results are written first to `matched_pairs.csv`, then merged into `anomaly_list.csv`
- Progress is printed every `100000` accepted rows
- Runtime statistics and memory samples are printed to stdout during execution
