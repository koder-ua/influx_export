import sys
import random
import datetime
from typing import Iterable, Dict, List, Tuple

from influxdb import InfluxDBClient

from .storage import RAWStorage


def gen_data() -> Iterable[Tuple[str, Dict[str, str], List[Tuple[str, float]]]]:
    tag1_vals = [f"tag1_{i}" for i in range(3)]
    tag2_vals = [f"tag2_{i}" for i in range(3)]
    step = datetime.timedelta(seconds=10)
    val_deltas = list(range(10000))
    measurements = [f"test_{name}" for name in "ABC"]
    start_time = datetime.datetime(2017, 2, 1, 0, 0, 10)
    val_step = 1000000
    cval = 0
    for measurement in measurements:
        for tag1 in tag1_vals:
            for tag2 in tag2_vals:
                ctime = start_time
                dt = []
                for val_delta in val_deltas:
                    dt.append((f"{ctime:%Y-%m-%dT%H:%M:%SZ}", cval + val_delta))
                    ctime += step
                yield measurement, {"tag1": tag1, "tag2": tag2}, dt
                cval += val_step


def insert_test_data():
    client = InfluxDBClient('localhost', 8086, 'scheduler', 'scheduler', 'scheduler')
    for meansurement, tags, data in gen_data():
        points = [
            {"measurement": meansurement,
             "tags": tags,
             "time": tm,
             "fields": {"value": val}}
             for tm, val in data
        ]
        client.write_points(points)


def insert_many_series():
    client = InfluxDBClient('localhost', 8086, 'scheduler', 'scheduler', 'scheduler')

    ctime = datetime.datetime(2017, 2, 1, 0, 0, 10)
    step = datetime.timedelta(seconds=10)

    data = []
    for didx in range(4):
        data.append([f"{ctime:%Y-%m-%dT%H:%M:%SZ}", didx])
        ctime += step

    tm = []
    for idx in range(20000):
        tvl = str(random.randint(0, 100))
        points = [
            {"measurement": f"measurement",
             "tags": {"tg": tvl, "idx": idx},
             "time": tm,
             "fields": {"value": val}}
             for tm, val in data
        ]
        client.write_points(points)


def test_downloaded_data():
    fpath = "/tmp/out.bin"
    all_data = []

    with RAWStorage.open(fpath, "r") as src:
        all_data.extend(src)

    expected = list(gen_data())
    expected_series = sorted((name, tuple(sorted(fields.items()))) for name, fields, _ in expected)
    have_series = sorted((serie.metric, tuple(sorted(serie.tags.items()))) for serie in all_data)
    assert expected_series == have_series

if sys.argv[1] == '-i':
    # insert_test_data()
    insert_many_series()
else:
    test_downloaded_data()
