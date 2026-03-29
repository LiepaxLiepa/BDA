from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import math
import multiprocessing as mp
from pathlib import Path
import sys
import time
from collections import deque
from typing import Iterable

SLOW_SHIP_SOG = 1.0
LOITERING_DISTANCE = 500.0
DRAUGHT_CHANGE = 0.05
IMPOSSIBLE_SPEED_KNOTS = 60.0
MIN_IMPOSSIBLE_SPEED_KNOTS_GAP_SECONDS = 60.0
PROGRESS_LOG_ROWS = 100_000
BATCH_SIZE = 100000
TIMESTAMP_FORMAT = "%d/%m/%Y %H:%M:%S"
QUEUE_WINDOW = timedelta(seconds=60)
LOITERING_HOURS = timedelta(hours=2)
GOING_DARK_HOURS = timedelta(hours=4)
GOING_DARK_MIN_DISTANCE_METERS = 1000.0
DRAUGHT_BLACKOUT_HOURS = timedelta(hours=2)
BUCKET_SIZE_DEGREES = 0.004492 #tiek laipsniu mazdaug sudaro 500 metru, tad galime pagal tai issiskaiciuoti bucketus
INVALID_MMSIS = {"000000000", "111111111", "123456789"}

#indeksai tam kad suprastume is kurios eilutes reikia imti reiksme, paprasciau naudoti toliau
TIMESTAMP_INDEX = 0
TYPE_OF_MOBILE_INDEX = 1
MMSI_INDEX = 2
LATITUDE_INDEX = 3
LONGITUDE_INDEX = 4
SOG_INDEX = 7
DRAUGHT_INDEX = 18

#loitering sekimui pasidarom pirma sarasa letu laivu 
@dataclass(frozen=True)
class SlowShipList:
    timestamp: datetime
    mmsi: str
    latitude: float
    longitude: float
    bucket_key: tuple[int, int]


@dataclass
class SlowPairList:
    mmsi_a: str
    mmsi_b: str
    first_matched_at: datetime
    last_confirmed_at: datetime
    emitted: bool = False


@dataclass
class AnomalyStats:
    going_dark_anomaly: bool = False
    draught_change: bool = False
    draught_change_count: int = 0
    impossible_speed: bool = False
    max_gap_hours: float = 0.0
    total_impossible_distance_nm: float = 0.0
    latest_anomaly_latitude: float | None = None
    latest_anomaly_longitude: float | None = None

    @property
    def dfsi(self) -> float:
        return (
            (self.max_gap_hours / 2.0)
            + (self.total_impossible_distance_nm / 10.0)
            + (self.draught_change_count * 15.0)
        )


@dataclass
class PairWorkerState:
    active_ships: deque[SlowShipList] = field(default_factory=deque)
    active_ships_by_mmsi: dict[str, SlowShipList] = field(default_factory=dict)
    spatial_buckets: dict[tuple[int, int], set[SlowShipList]] = field(default_factory=dict)
    tracked_pairs: dict[tuple[str, str], SlowPairList] = field(default_factory=dict)
    pair_keys_by_mmsi: dict[str, set[tuple[str, str]]] = field(default_factory=dict)


@dataclass
class AnomalyWorkerState:
    last_seen_by_mmsi: dict[str, datetime] = field(default_factory=dict)
    last_draught_by_mmsi: dict[str, float] = field(default_factory=dict)
    last_coordinates_by_mmsi: dict[str, tuple[float, float]] = field(default_factory=dict)
    emitted_anomaly_keys: set[tuple[str, str]] = field(default_factory=set)
    stats_by_mmsi: dict[str, AnomalyStats] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
    )
    parser.add_argument(
        "--input-file",
        dest="input_files",
        action="append",
        required=True,
    )
    return parser.parse_args()

def calculate_distance_meters(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    earth_radius_m = 6_371_000.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    arc = 2 * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))
    return earth_radius_m * arc


def lat_lon_validation(row: tuple[str, ...]) -> tuple[float, float] | None:
    try:
        latitude = float(row[LATITUDE_INDEX])
        longitude = float(row[LONGITUDE_INDEX])
    except ValueError:
        return None

    if not (-90.0 <= latitude <= 90.0):
        return None
    if not (-180.0 <= longitude <= 180.0):
        return None
    
    return latitude, longitude


def mmsi_is_usable(mmsi: str) -> bool:
    if not mmsi:
        return False
    if len(mmsi) != 9 or not mmsi.isdigit():
        return False
    if mmsi in INVALID_MMSIS:
        return False
    if len(set(mmsi)) == 1:
        return False
    return True


def pair_order_validation(mmsi_a: str, mmsi_b: str) -> tuple[str, str]:
    if mmsi_a < mmsi_b:
        return (mmsi_a, mmsi_b)
    return (mmsi_b, mmsi_a)


def make_bucket_key(latitude: float, longitude: float) -> tuple[int, int]:
    return (
        math.floor(latitude / BUCKET_SIZE_DEGREES),
        math.floor(longitude / BUCKET_SIZE_DEGREES),
    )

def check_neighbor_buckets(bucket_key: tuple[int, int]) -> Iterable[tuple[int, int]]:
    lat_bucket, lon_bucket = bucket_key
    for lat_offset in range(-1, 2):
        for lon_offset in range(-1, 2):
            yield (lat_bucket + lat_offset, lon_bucket + lon_offset)


def remove_from_mmsi_pairs(
    mmsi: str,
    tracked_pairs: dict[tuple[str, str], SlowPairList],
    pair_keys_by_mmsi: dict[str, set[tuple[str, str]]],
) -> None:
    pair_keys = pair_keys_by_mmsi.pop(mmsi, set())
    for pair_key in pair_keys:
        pair_state = tracked_pairs.pop(pair_key, None)
        if pair_state is None:
            continue

        other_mmsi = pair_state.mmsi_b if pair_state.mmsi_a == mmsi else pair_state.mmsi_a
        other_pairs = pair_keys_by_mmsi.get(other_mmsi)
        if other_pairs is not None:
            other_pairs.discard(pair_key)
            if not other_pairs:
                del pair_keys_by_mmsi[other_mmsi]


def near_ship_pairs(
    current_mmsi: str,
    other_mmsi: str,
    timestamp: datetime,
    tracked_pairs: dict[tuple[str, str], SlowPairList],
    pair_keys_by_mmsi: dict[str, set[tuple[str, str]]],
) -> SlowPairList:
    pair_key = pair_order_validation(current_mmsi, other_mmsi)
    pair_state = tracked_pairs.get(pair_key)
    if pair_state is None:
        pair_state = SlowPairList(
            mmsi_a=pair_key[0],
            mmsi_b=pair_key[1],
            first_matched_at=timestamp,
            last_confirmed_at=timestamp,
        )
        tracked_pairs[pair_key] = pair_state
        pair_keys_by_mmsi.setdefault(pair_key[0], set()).add(pair_key)
        pair_keys_by_mmsi.setdefault(pair_key[1], set()).add(pair_key)
        return pair_state

    pair_state.last_confirmed_at = timestamp
    return pair_state


def invalidate_pair(
    pair_key: tuple[str, str],
    tracked_pairs: dict[tuple[str, str], SlowPairList],
    pair_keys_by_mmsi: dict[str, set[tuple[str, str]]],
    reason: str,
) -> None:
    pair_state = tracked_pairs.pop(pair_key, None)
    if pair_state is None:
        return

    for mmsi in (pair_state.mmsi_a, pair_state.mmsi_b):
        mmsi_pairs = pair_keys_by_mmsi.get(mmsi)
        if mmsi_pairs is None:
            continue
        mmsi_pairs.discard(pair_key)
        if not mmsi_pairs:
            del pair_keys_by_mmsi[mmsi]


def emit_verified_pair(pair_state: SlowPairList, writer: csv.writer) -> None:
    start = pair_state.first_matched_at.strftime(TIMESTAMP_FORMAT)
    end = pair_state.last_confirmed_at.strftime(TIMESTAMP_FORMAT)

    writer.writerow((pair_state.mmsi_a, pair_state.mmsi_b, start, end))


def get_anomaly_stats(
    stats_by_mmsi: dict[str, AnomalyStats], mmsi: str
) -> AnomalyStats:
    stats = stats_by_mmsi.get(mmsi)
    if stats is None:
        stats = AnomalyStats()
        stats_by_mmsi[mmsi] = stats
    return stats


def update_max_gap_hours(stats: AnomalyStats, previous_timestamp: datetime, current_timestamp: datetime) -> None:
    gap_hours = (current_timestamp - previous_timestamp).total_seconds() / 3600.0
    if gap_hours > stats.max_gap_hours:
        stats.max_gap_hours = gap_hours


def update_latest_anomaly_coordinates(
    stats: AnomalyStats, current_coordinates: tuple[float, float] | None
) -> None:
    if current_coordinates is None:
        return
    stats.latest_anomaly_latitude = current_coordinates[0]
    stats.latest_anomaly_longitude = current_coordinates[1]


def write_anomaly_summary(output_path: Path, stats_by_mmsi: dict[str, AnomalyStats]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            (
                "mmsi",
                "going_dark_anomaly",
                "draught_change",
                "draught_change_count",
                "impossible_speed",
                "max_gap_hours",
                "total_impossible_distance_nm",
                "latest_anomaly_latitude",
                "latest_anomaly_longitude",
                "dfsi",
            )
        )
        for mmsi in sorted(stats_by_mmsi):
            stats = stats_by_mmsi[mmsi]
            writer.writerow(
                (
                    mmsi,
                    stats.going_dark_anomaly,
                    stats.draught_change,
                    stats.draught_change_count,
                    stats.impossible_speed,
                    f"{stats.max_gap_hours:.3f}",
                    f"{stats.total_impossible_distance_nm:.3f}",
                    ""
                    if stats.latest_anomaly_latitude is None
                    else f"{stats.latest_anomaly_latitude:.6f}",
                    ""
                    if stats.latest_anomaly_longitude is None
                    else f"{stats.latest_anomaly_longitude:.6f}",
                    f"{stats.dfsi:.3f}",
                )
            )


def draught_validation(row: tuple[str, ...]) -> float | None:
    raw_value = row[DRAUGHT_INDEX].strip()
    if not raw_value:
        return None

    try:
        draught = float(raw_value)
    except ValueError:
        return None

    if draught < 0:
        return None

    return draught


def process_pair_row(
    row: tuple[str, ...],
    writer: csv.writer,
    state: PairWorkerState,
) -> None:
    active_ships = state.active_ships
    active_ships_by_mmsi = state.active_ships_by_mmsi
    spatial_buckets = state.spatial_buckets
    tracked_pairs = state.tracked_pairs
    pair_keys_by_mmsi = state.pair_keys_by_mmsi

    mobile_type = row[TYPE_OF_MOBILE_INDEX]
    if mobile_type not in {"Class A", "Class B"}:
        return

    sog_value = row[SOG_INDEX]
    if not sog_value:
        return

    try:
        sog = float(sog_value)
    except ValueError:
        return

    if sog >= SLOW_SHIP_SOG:
        return

    coordinates = lat_lon_validation(row)
    if coordinates is None:
        return
    current_latitude, current_longitude = coordinates

    timestamp = datetime.strptime(row[TIMESTAMP_INDEX], TIMESTAMP_FORMAT)
    mmsi = row[MMSI_INDEX]
    window_start = timestamp - QUEUE_WINDOW

    while active_ships and active_ships[0].timestamp < window_start:
        stored_entry = active_ships.popleft()
        bucket_entries = spatial_buckets.get(stored_entry.bucket_key)
        if bucket_entries is not None:
            bucket_entries.discard(stored_entry)
            if not bucket_entries:
                del spatial_buckets[stored_entry.bucket_key]
        current_entry = active_ships_by_mmsi.get(stored_entry.mmsi)
        if current_entry is stored_entry:
            del active_ships_by_mmsi[stored_entry.mmsi]
            remove_from_mmsi_pairs(
                stored_entry.mmsi, tracked_pairs, pair_keys_by_mmsi
            )

    bucket_key = make_bucket_key(current_latitude, current_longitude)
    current_entry = SlowShipList(
        timestamp=timestamp,
        mmsi=mmsi,
        latitude=current_latitude,
        longitude=current_longitude,
        bucket_key=bucket_key,
    )
    active_ships_by_mmsi[mmsi] = current_entry

    for neighbor_bucket_key in check_neighbor_buckets(bucket_key):
        for previous_ship in spatial_buckets.get(neighbor_bucket_key, ()):
            if active_ships_by_mmsi.get(previous_ship.mmsi) is not previous_ship:
                continue

            if previous_ship.mmsi == mmsi:
                continue

            distance_meters = calculate_distance_meters(
                previous_ship.latitude,
                previous_ship.longitude,
                current_latitude,
                current_longitude,
            )
            pair_key = pair_order_validation(previous_ship.mmsi, mmsi)
            if distance_meters < LOITERING_DISTANCE:
                pair_state = near_ship_pairs(
                    current_mmsi=mmsi,
                    other_mmsi=previous_ship.mmsi,
                    timestamp=timestamp,
                    tracked_pairs=tracked_pairs,
                    pair_keys_by_mmsi=pair_keys_by_mmsi,
                )
                if (
                    not pair_state.emitted
                    and pair_state.last_confirmed_at - pair_state.first_matched_at
                    >= LOITERING_HOURS
                ):
                    pair_state.emitted = True
                    emit_verified_pair(pair_state, writer)
            else:
                invalidate_pair(
                    pair_key,
                    tracked_pairs,
                    pair_keys_by_mmsi,
                    reason="distance_broken",
                )

    active_ships.append(current_entry)
    spatial_buckets.setdefault(bucket_key, set()).add(current_entry)


def process_anomaly_row(
    row: tuple[str, ...],
    state: AnomalyWorkerState,
) -> None:
    last_seen_by_mmsi = state.last_seen_by_mmsi
    last_draught_by_mmsi = state.last_draught_by_mmsi
    last_coordinates_by_mmsi = state.last_coordinates_by_mmsi
    emitted_anomaly_keys = state.emitted_anomaly_keys
    stats_by_mmsi = state.stats_by_mmsi

    timestamp = datetime.strptime(row[TIMESTAMP_INDEX], TIMESTAMP_FORMAT)
    mmsi = row[MMSI_INDEX].strip()
    if not mmsi:
        return
    current_coordinates = lat_lon_validation(row)

    previous_timestamp = last_seen_by_mmsi.get(mmsi)
    previous_coordinates = last_coordinates_by_mmsi.get(mmsi)
    if (
        ("going_dark", mmsi) not in emitted_anomaly_keys
        and previous_timestamp is not None
        and timestamp - previous_timestamp > GOING_DARK_HOURS
        and previous_coordinates is not None
        and current_coordinates is not None
        and calculate_distance_meters(
            previous_coordinates[0],
            previous_coordinates[1],
            current_coordinates[0],
            current_coordinates[1],
        ) > GOING_DARK_MIN_DISTANCE_METERS
    ):
        stats = get_anomaly_stats(stats_by_mmsi, mmsi)
        stats.going_dark_anomaly = True
        update_max_gap_hours(stats, previous_timestamp, timestamp)
        update_latest_anomaly_coordinates(stats, current_coordinates)
        emitted_anomaly_keys.add(("going_dark", mmsi))

    current_draught = draught_validation(row)
    previous_draught = last_draught_by_mmsi.get(mmsi)
    if (
        previous_timestamp is not None
        and previous_draught is not None
        and current_draught is not None
        and previous_draught > 0
        and timestamp - previous_timestamp > DRAUGHT_BLACKOUT_HOURS
    ):
        draught_change_percent = abs(current_draught - previous_draught) / previous_draught
        if draught_change_percent > DRAUGHT_CHANGE:
            stats = get_anomaly_stats(stats_by_mmsi, mmsi)
            stats.draught_change = True
            stats.draught_change_count += 1
            update_max_gap_hours(stats, previous_timestamp, timestamp)
            update_latest_anomaly_coordinates(stats, current_coordinates)

    if (
        ("IMPOSSIBLE_SPEED_KNOTS_same_mmsi", mmsi) not in emitted_anomaly_keys
        and previous_timestamp is not None
        and previous_coordinates is not None
        and current_coordinates is not None
    ):
        gap_seconds = (timestamp - previous_timestamp).total_seconds()
        if gap_seconds > MIN_IMPOSSIBLE_SPEED_KNOTS_GAP_SECONDS:
            distance_meters = calculate_distance_meters(
                previous_coordinates[0],
                previous_coordinates[1],
                current_coordinates[0],
                current_coordinates[1],
            )
            calculated_speed_knots = (distance_meters / 1852.0) / (
                gap_seconds / 3600.0
            )
            if calculated_speed_knots > IMPOSSIBLE_SPEED_KNOTS:
                stats = get_anomaly_stats(stats_by_mmsi, mmsi)
                stats.impossible_speed = True
                stats.total_impossible_distance_nm += distance_meters / 1852.0
                update_latest_anomaly_coordinates(stats, current_coordinates)
                emitted_anomaly_keys.add(("IMPOSSIBLE_SPEED_KNOTS_same_mmsi", mmsi))

    last_seen_by_mmsi[mmsi] = timestamp
    if current_draught is not None:
        last_draught_by_mmsi[mmsi] = current_draught
    if current_coordinates is not None:
        last_coordinates_by_mmsi[mmsi] = current_coordinates


def second_worker(queue: mp.Queue, output_path: str) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    state = PairWorkerState()

    with output_file.open("w", newline="", encoding="utf-8", buffering=1) as handle:
        writer = csv.writer(handle)
        writer.writerow(("mmsi_a", "mmsi_b", "start", "end"))

        while True:
            batch = queue.get()
            if batch is None:
                break
            for row in batch:
                process_pair_row(row, writer, state)


def first_worker(queue: mp.Queue, output_path: str) -> None:
    output_file = Path(output_path)
    state = AnomalyWorkerState()

    while True:
        batch = queue.get()
        if batch is None:
            break

        for row in batch:
            process_anomaly_row(row, state)

    write_anomaly_summary(output_file, state.stats_by_mmsi)


def stream_csv_rows(csv_path: Path) -> Iterable[tuple[str, ...]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return

        for row in reader:
            yield tuple(row)


def print_runtime(started_at: datetime, finished_at: datetime, elapsed_seconds: float) -> None:
    print(
        "PROGRAM_RUNTIME "
        f"start={started_at.strftime(TIMESTAMP_FORMAT)} "
        f"end={finished_at.strftime(TIMESTAMP_FORMAT)} "
        f"duration_seconds={elapsed_seconds:.2f}",
        file=sys.stderr,
    )

def print_progress(rows_read: int) -> None:
    print(
        f"PROGRESS rows_read={rows_read}",
        file=sys.stderr,
    )


def main() -> int:
    started_at = datetime.now()
    started_perf = time.perf_counter()
    try:
        args = parse_args()
        input_paths = [Path(value) for value in args.input_files]
        output_path = Path("matched_pairs.csv")
        anomaly_output_path = Path("anomaly_list.csv")

        pair_queue: mp.Queue = mp.Queue()
        anomaly_queue: mp.Queue = mp.Queue()
        pair_worker = mp.Process(target=second_worker, args=(pair_queue, str(output_path)))
        anomaly_worker = mp.Process(
            target=first_worker,
            args=(anomaly_queue, str(anomaly_output_path)),
        )
        pair_worker.start()
        anomaly_worker.start()

        try:
            batch: list[tuple[str, ...]] = []
            rows_read = 0
            for input_path in input_paths:
                for row in stream_csv_rows(input_path):
                    if not mmsi_is_usable(row[MMSI_INDEX].strip()):
                        continue
                    rows_read += 1
                    batch.append(row)
                    if rows_read % PROGRESS_LOG_ROWS == 0:
                        print_progress(rows_read)
                    if len(batch) >= BATCH_SIZE:
                        batch_payload = tuple(batch)
                        pair_queue.put(batch_payload)
                        anomaly_queue.put(batch_payload)
                        batch.clear()
            if batch:
                batch_payload = tuple(batch)
                pair_queue.put(batch_payload)
                anomaly_queue.put(batch_payload)
        finally:
            pair_queue.put(None)
            anomaly_queue.put(None)
            pair_worker.join()
            anomaly_worker.join()

        if pair_worker.exitcode != 0:
            raise RuntimeError(f"Pair worker exited with code {pair_worker.exitcode}")
        if anomaly_worker.exitcode != 0:
            raise RuntimeError(
                f"Anomaly worker exited with code {anomaly_worker.exitcode}"
            )

        return 0
    finally:
        finished_at = datetime.now()
        elapsed_seconds = time.perf_counter() - started_perf
        print_runtime(started_at, finished_at, elapsed_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
