from __future__ import annotations

import argparse
import csv
import math
import os
import shutil
import time
import zlib
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import psutil

# =========================
# CONFIG
# =========================

INVALID_MMSI = {
    "000000000",
    "111111111",
    "123456789",
    "999999999",
}
MIN_VALID_MMSI = 100000000
MAX_VALID_MMSI = 999999998


GOING_DARK_HOURS = 4.0
LOITER_DISTANCE_METERS = 500.0
LOITER_MAX_SOG = 1.0
LOITER_TIME = 2.0 
#LOITER TIME irgi gaunasi kad niekur nepanauojam, nes toliau kode nėra 
DRAFT_BLACKOUT_HOURS = 2.0
DEPTH_IN_WATER = 0.05
IMPOSSIBLE_SPEED_KNOTS = 60.0


# =========================
# FIXES
# =========================

def fix_mb(value: int) -> float:
    return value / (1024 * 1024)

#fix so that code does not get stuck on not floats or n/as
def safe_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

#convert a timestamp string from the CSV into a datetime object
def parse_timestamp(text):
    try:
        text = text.strip()
        # Try ISO format first (from partitioned files)
        try:
            return datetime.fromisoformat(text)
        except:
            # Fall back to original format (from raw CSV)
            return datetime.strptime(text, "%d/%m/%Y %H:%M:%S")
    except:
        return None


def is_valid_mmsi(mmsi: str) -> bool:
    if not mmsi:
        return False
    mmsi = str(mmsi).strip()
    if mmsi in INVALID_MMSI:
        return False
    if len(mmsi) != 9 or not mmsi.isdigit():
        return False
    val = int(mmsi)
    return MIN_VALID_MMSI <= val <= MAX_VALID_MMSI

#calculates the distance between two geographic coordinates
def distance(lat1, lon1, lat2, lon2):
    r = 6371000.0 #average radius in meters
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a)) #convert into meters

#convert meters to nautical miles
def meters_to_nm(meters):
    return meters / 1852.0

#calculates how many hours passed between two timestamps
def hours_between(t1, t2):
    return abs((t2 - t1).total_seconds()) / 3600.0

#decides which worker a piece of data should go to during parallel processing
#ensures that same MMSI → always same worker
def worker(key, n):
    return zlib.crc32(str(key).encode("utf-8")) % n

#redefine the header names for some columns into shorter ones
def build_header_map(fieldnames):
    return {
        "mmsi": "MMSI",
        "ts": "# Timestamp",
        "lat": "Latitude",
        "lon": "Longitude",
        "draught": "Draught",
        "sog": "SOG"
    }


def dfsi_score(max_gap_hours, total_impossible_jump_nm, c_count):
    return (max_gap_hours / 2.0) + (total_impossible_jump_nm / 10.0) + (c_count * 15.0)


#loops through both files and returns the total number of rows.
def count_rows(input_files: List[Path]) -> int:
    total = 0
    for path in input_files:
        with path.open("r", encoding="utf-8", newline="") as f:
            total += sum(1 for _ in f) - 1 #counts every line in the file exept the header
    return total



# ==========================
# Task 1: Low-Memory Parallel Partitioning
    #1) cleans bad rows
    #2) sends each valid row into a smaller partition file
    #3) makes sure the same vessel always goes to the same bucket
    #4) creates a second set of partition files for loitering detection
# =========================

def partition_files(input_csv_files: List[Path], workdir: Path, n_buckets: int, progress_chunk: int) -> Tuple[int, int, int]:
    """Pre-sorted partitioning with buffered writes"""
    os.makedirs(workdir, exist_ok=True)
    part_dir = workdir / "parts"
    loiter_dir = workdir / "loiter"
    part_dir.mkdir(parents=True, exist_ok=True)
    loiter_dir.mkdir(parents=True, exist_ok=True)

    normalized_header = ["mmsi", "ts", "lat", "lon", "sog", "draught"]
    
    # Use DictWriter with buffering (write in chunks)
    part_files = {}
    loiter_files = {}
    part_writers = {}
    loiter_writers = {}
    write_buffers_part = {}
    write_buffers_loiter = {}
    
    BUFFER_SIZE = 5000  # Batch writes
    
    for i in range(n_buckets):
        part_files[i] = open(part_dir / f"part_{i}.csv", "w", newline="", encoding="utf-8", buffering=65536)
        loiter_files[i] = open(loiter_dir / f"loiter_{i}.csv", "w", newline="", encoding="utf-8", buffering=65536)
        
        part_writers[i] = csv.DictWriter(part_files[i], fieldnames=normalized_header)
        loiter_writers[i] = csv.DictWriter(loiter_files[i], fieldnames=normalized_header)
        
        part_writers[i].writeheader()
        loiter_writers[i].writeheader()
        
        write_buffers_part[i] = []
        write_buffers_loiter[i] = []

    rows_seen = 0
    rows_kept = 0
    dirty_rows = 0

    for input_csv in input_csv_files:
        print(f"[partition] reading {input_csv.name}")

        with input_csv.open("r", newline="", encoding="utf-8", errors="replace", buffering=65536) as f:
            reader = csv.DictReader(f)
            header_map = build_header_map(reader.fieldnames or [])

            required = ["mmsi", "ts", "lat", "lon"]
            missing = [x for x in required if header_map.get(x) is None]
            if missing:
                raise ValueError(f"Missing columns {missing} in file {input_csv}")

            for row in reader:
                rows_seen += 1

                mmsi = str(row.get(header_map["mmsi"], "")).strip()
                ts = parse_timestamp(row.get(header_map["ts"]))
                lat = safe_float(row.get(header_map["lat"]))
                lon = safe_float(row.get(header_map["lon"]))
                sog = safe_float(row.get(header_map.get("sog")), default=0.0)
                draught = safe_float(row.get(header_map.get("draught")), default=None)

                if not is_valid_mmsi(mmsi) or ts is None or lat is None or lon is None:
                    dirty_rows += 1
                    continue
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    dirty_rows += 1
                    continue

                if sog is None or sog < 0 or sog > 100:
                    sog = 0.0
                if draught is not None and draught < 0:
                    draught = None

                norm = {
                    "mmsi": mmsi,
                    "ts": ts.isoformat(sep=" "),
                    "lat": lat,
                    "lon": lon,
                    "sog": sog,
                    "draught": draught if draught is not None else ""
                }

                b = worker(mmsi, n_buckets)
                write_buffers_part[b].append(norm)
                rows_kept += 1

                if sog <= LOITER_MAX_SOG:
                    lb = worker(ts.strftime("%Y-%m-%d %H"), n_buckets)
                    write_buffers_loiter[lb].append(norm)

                # Flush buffers
                if len(write_buffers_part[b]) >= BUFFER_SIZE:
                    part_writers[b].writerows(write_buffers_part[b])
                    write_buffers_part[b] = []

                if sog <= LOITER_MAX_SOG and len(write_buffers_loiter[lb]) >= BUFFER_SIZE:
                    loiter_writers[lb].writerows(write_buffers_loiter[lb])
                    write_buffers_loiter[lb] = []

                if rows_seen % progress_chunk == 0:
                    print(f"[partition] rows_seen={rows_seen:,} rows_kept={rows_kept:,} dirty={dirty_rows:,}")

    # Flush remaining buffers
    for i in range(n_buckets):
        if write_buffers_part[i]:
            part_writers[i].writerows(write_buffers_part[i])
        if write_buffers_loiter[i]:
            loiter_writers[i].writerows(write_buffers_loiter[i])

    for f in list(part_files.values()) + list(loiter_files.values()):
        f.close()

    return rows_seen, rows_kept, dirty_rows


# =========================
# OPTIMIZATION 2: Pre-sort partitions, cache computations
# =========================

def process_main_partition(task: Tuple[int, str]) -> Tuple[int, float, Dict[str, dict]]:
    chunk_index, path = task
    start_time = time.perf_counter()

    # Read ALL data for partition into memory (each partition is small)
    vessels = defaultdict(list)

    with open(path, "r", newline="", encoding="utf-8", buffering=65536) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_str = row.get("ts")
            ts = parse_timestamp(ts_str)
            if ts is None:
                continue

            lat = safe_float(row.get("lat"))
            lon = safe_float(row.get("lon"))
            sog = safe_float(row.get("sog"), default=0.0)
            draught_str = row.get("draught")
            draught = safe_float(draught_str) if draught_str and draught_str.strip() else None
            mmsi = row.get("mmsi")

            if lat is None or lon is None:
                continue

            vessels[mmsi].append((ts, lat, lon, sog, draught))

    results = {}

    for mmsi, pts in vessels.items():
        if len(pts) < 2:
            continue

        # Pre-sort (cheap since partitions are small)
        pts.sort(key=lambda x: x[0])

        a_count = 0
        c_count = 0
        d_count = 0
        max_gap_hours = 0.0
        total_impossible_jump_nm = 0.0
        first_flag_coord = None

        # Cache distance calculation
        prev_t, prev_lat, prev_lon, prev_sog, prev_dr = pts[0]

        for i in range(1, len(pts)):
            curr_t, curr_lat, curr_lon, curr_sog, curr_dr = pts[i]

            gap_h = hours_between(prev_t, curr_t)
            
            if gap_h == 0:  # Skip same-timestamp pairs
                prev_t, prev_lat, prev_lon, prev_sog, prev_dr = curr_t, curr_lat, curr_lon, curr_sog, curr_dr
                continue

            # Only calculate distance once
            dist_m = distance(prev_lat, prev_lon, curr_lat, curr_lon)
            dist_nm = meters_to_nm(dist_m)
            implied_speed = dist_nm / gap_h

            # Check A: Going dark
            if gap_h > GOING_DARK_HOURS and implied_speed > 1.0:
                a_count += 1
                max_gap_hours = max(max_gap_hours, gap_h)
                if first_flag_coord is None:
                    first_flag_coord = (curr_lat, curr_lon)

            # Check C: Draft change (only if both have draft data)
            if (gap_h > DRAFT_BLACKOUT_HOURS and 
                prev_dr is not None and curr_dr is not None and prev_dr > 0):
                change_pct = abs(curr_dr - prev_dr) / prev_dr
                if change_pct > 0.05:
                    c_count += 1
                    if first_flag_coord is None:
                        first_flag_coord = (curr_lat, curr_lon)

            # Check D: Impossible speed
            if implied_speed > IMPOSSIBLE_SPEED_KNOTS:
                d_count += 1
                total_impossible_jump_nm += dist_nm
                if first_flag_coord is None:
                    first_flag_coord = (curr_lat, curr_lon)

            prev_t, prev_lat, prev_lon, prev_sog, prev_dr = curr_t, curr_lat, curr_lon, curr_sog, curr_dr

        # Only add if anomaly detected
        if a_count > 0 or c_count > 0 or d_count > 0:
            results[mmsi] = {
                "A": a_count,
                "C": c_count,
                "D": d_count,
                "max_gap_hours": max_gap_hours,
                "total_impossible_jump_nm": total_impossible_jump_nm,
                "flag_lat": first_flag_coord[0] if first_flag_coord else None,
                "flag_lon": first_flag_coord[1] if first_flag_coord else None,
            }

    elapsed_time = time.perf_counter() - start_time
    return chunk_index, elapsed_time, results


# =========================
# OPTIMIZATION 3: Spatial hash instead of O(n²) pairs
# =========================

def process_loiter_partition(task: Tuple[int, str]) -> Tuple[int, float, Dict[str, int]]:
    chunk_index, path = task
    start_time = time.perf_counter()

    by_hour = defaultdict(list)

    with open(path, "r", newline="", encoding="utf-8", buffering=65536) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_timestamp(row["ts"])
            if ts is None:
                continue

            mmsi = row["mmsi"]
            lat = safe_float(row["lat"])
            lon = safe_float(row["lon"])

            if lat is None or lon is None:
                continue

            hour_key = ts.strftime("%Y-%m-%d %H")
            by_hour[hour_key].append((mmsi, lat, lon))

    vessel_b_counts = defaultdict(int)
    pair_hours = defaultdict(list)

    # Grid-based spatial hash instead of O(n²)
    GRID_SIZE = 0.005  # ~0.500 km cells 

    for hour_key, rows in sorted(by_hour.items()):
        # Build spatial grid
        grid = defaultdict(list)
        for mmsi, lat, lon in rows:
            grid_x = int(lat / GRID_SIZE)
            grid_y = int(lon / GRID_SIZE)
            grid[(grid_x, grid_y)].append((mmsi, lat, lon))

        # Check only neighboring grid cells
        seen_pairs = set()
        checked_cells = set()
        
        for (gx, gy) in grid.keys():
            if (gx, gy) in checked_cells:
                continue
                
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    neighbor_key = (gx + dx, gy + dy)
                    if neighbor_key not in grid or neighbor_key in checked_cells:
                        continue
                    
                    cells_a = grid[(gx, gy)]
                    cells_b = grid[neighbor_key]
                    
                    for m1, lat1, lon1 in cells_a:
                        for m2, lat2, lon2 in cells_b:
                            if m1 >= m2:  # Skip duplicates
                                continue
                            
                            dist = distance(lat1, lon1, lat2, lon2)
                            if dist <= LOITER_DISTANCE_METERS:
                                pair = (m1, m2)
                                if pair not in seen_pairs:
                                    pair_hours[pair].append(hour_key)
                                    seen_pairs.add(pair)
            
            checked_cells.add((gx, gy))

    # Count consecutive hours
    def parse_hour(hour_str):
        return datetime.strptime(hour_str, "%Y-%m-%d %H")

    for pair, hours_list in pair_hours.items():
        hours_list.sort()
        
        prev_time = None
        consecutive = 0

        for hour_str in hours_list:
            curr_time = parse_hour(hour_str)

            if prev_time is not None and curr_time - prev_time == timedelta(hours=1):
                consecutive += 1
            else:
                consecutive = 1

            if consecutive >= 2:
                m1, m2 = pair
                vessel_b_counts[m1] += 1
                vessel_b_counts[m2] += 1
                break

            prev_time = curr_time

    elapsed_time = time.perf_counter() - start_time
    return chunk_index, elapsed_time, dict(vessel_b_counts)


# =========================
# MERGE
# =========================

def merge_results(main_results_list, loiter_results_list):
    merged = defaultdict(lambda: {
        "A": 0,
        "B": 0,
        "C": 0,
        "D": 0,
        "max_gap_hours": 0.0,
        "total_impossible_jump_nm": 0.0,
        "flag_lat": None,
        "flag_lon": None,
    })

    for result_dict in main_results_list:
        for mmsi, data in result_dict.items():
            merged[mmsi]["A"] += data["A"]
            merged[mmsi]["C"] += data["C"]
            merged[mmsi]["D"] += data["D"]
            merged[mmsi]["max_gap_hours"] = max(merged[mmsi]["max_gap_hours"], data["max_gap_hours"])
            merged[mmsi]["total_impossible_jump_nm"] += data["total_impossible_jump_nm"]

            if merged[mmsi]["flag_lat"] is None and data["flag_lat"] is not None:
                merged[mmsi]["flag_lat"] = data["flag_lat"]
                merged[mmsi]["flag_lon"] = data["flag_lon"]

    for loiter_dict in loiter_results_list:
        for mmsi, b_count in loiter_dict.items():
            merged[mmsi]["B"] += b_count

    final_rows = []
    for mmsi, d in merged.items():
        score = dfsi_score(d["max_gap_hours"], d["total_impossible_jump_nm"], d["C"])
        final_rows.append({
            "mmsi": mmsi,
            "A_going_dark": d["A"],
            "B_loiter_transfer": d["B"],
            "C_draft_change": d["C"],
            "D_cloning": d["D"],
            "max_gap_hours": round(d["max_gap_hours"], 3),
            "total_impossible_jump_nm": round(d["total_impossible_jump_nm"], 3),
            "DFSI": round(score, 3),
            "flag_lat": d["flag_lat"],
            "flag_lon": d["flag_lon"],
        })

    final_rows.sort(key=lambda x: (-x["DFSI"], -x["C_draft_change"], -x["D_cloning"]))
    return final_rows


def write_results_csv(rows, output_file: Path):
    with output_file.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "mmsi",
                "A_going_dark",
                "B_loiter_transfer",
                "C_draft_change",
                "D_cloning",
                "max_gap_hours",
                "total_impossible_jump_nm",
                "DFSI",
                "flag_lat",
                "flag_lon",
            ]
        )
        writer.writeheader()
        writer.writerows(rows)


# =========================
# MAIN PIPELINE
# =========================

def run_pipeline(input_files: List[Path], output_file: Path, workdir: Path, workers: int, chunk_size: int, keep_temp: bool):
    start = time.perf_counter()
    process = psutil.Process()

    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

   

    rows_seen, rows_kept, dirty_rows = partition_files(
        input_csv_files=input_files,
        workdir=workdir,
        n_buckets=workers,
        progress_chunk=chunk_size
    )

    print(f"\nPartition done")
    print(f"Rows seen:  {rows_seen:,}")
    print(f"Rows kept:  {rows_kept:,}")
    print(f"Dirty rows: {dirty_rows:,}")
    print(f"Memory RSS after partition: {fix_mb(process.memory_info().rss):.2f} MB")

    part_files = sorted((workdir / "parts").glob("part_*.csv"))
    loiter_files = sorted((workdir / "loiter").glob("loiter_*.csv"))

    main_tasks = [(i + 1, str(path)) for i, path in enumerate(part_files)]
    loiter_tasks = [(i + 1, str(path)) for i, path in enumerate(loiter_files)]

    main_results_list = []
    loiter_results_list = []

    completed_chunks = 0
    total_chunks = len(main_tasks) + len(loiter_tasks)

    with Pool(processes=workers) as pool:
        for chunk_index, chunk_time, partial in pool.imap_unordered(process_main_partition, main_tasks, chunksize=1):
            main_results_list.append(partial)
            completed_chunks += 1
            mem_rss = process.memory_info().rss
            print(
                f"[main] chunk {chunk_index} processed in {chunk_time:.4f} s | "
                f"progress {completed_chunks}/{total_chunks} | "
                f"memory RSS {fix_mb(mem_rss):.2f} MB"
            )

    with Pool(processes=workers) as pool:
        for chunk_index, chunk_time, partial in pool.imap_unordered(process_loiter_partition, loiter_tasks, chunksize=1):
            loiter_results_list.append(partial)
            completed_chunks += 1
            mem_rss = process.memory_info().rss
            print(
                f"[loiter] chunk {chunk_index} processed in {chunk_time:.4f} s | "
                f"progress {completed_chunks}/{total_chunks} | "
                f"memory RSS {fix_mb(mem_rss):.2f} MB"
            )

    final_rows = merge_results(main_results_list, loiter_results_list)
    write_results_csv(final_rows, output_file)

    total_time = time.perf_counter() - start
    final_mem_rss = process.memory_info().rss

    print(f"\nResults written to: {output_file.name}")
    print(f"Flagged vessels: {len(final_rows)}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Final memory RSS: {fix_mb(final_mem_rss):.2f} MB")

    print("\nTop 5 vessels by DFSI:")
    for row in final_rows[:5]:
        print(row)

    if not keep_temp:
        shutil.rmtree(workdir, ignore_errors=True)


# =========================
# CLI
# =========================

def main():
    parser = argparse.ArgumentParser(description="Shadow Fleet detection for two AIS days")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Example: aisdk-2025-05-02.csv aisdk-2025-05-03.csv"
    )
    parser.add_argument("--output", default="shadow_fleet_results.csv")
    parser.add_argument("--workdir", default="shadow_work")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunk-size", type=int, default=100000)
    parser.add_argument("--keep-temp", action="store_true")

    args = parser.parse_args()

    input_files = [Path(x) for x in args.inputs]
    output_file = Path(args.output)
    workdir = Path(args.workdir)

    run_pipeline(
        input_files=input_files,
        output_file=output_file,
        workdir=workdir,
        workers=args.workers,
        chunk_size=args.chunk_size,
        keep_temp=args.keep_temp,
    )


if __name__ == "__main__":
    main()

