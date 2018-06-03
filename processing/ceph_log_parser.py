import re
import sys
import time
import pathlib
import datetime


rest_map = {
    'pgmap': None,
    'monmap': None,
    'fsmap': None,
    'osdmap': None,
    'slow': None,
}

pg_name_re = re.compile(r"\d+\.[0-9a-fA-F]+$")
deep_ends = {}
scrub_ends = {}
deep_start = {}
scrub_start = {}

for line in pathlib.Path(sys.argv[1]).expanduser().open():
    ln = line.strip()
    if not ln:
        continue
    try:
        date, tm, src, addr, num1, column, src2, level, rest1, *rest = ln.split(" ", 9)
    except:
        print(ln)
        raise
    if date == '2018-05-25':
        if rest1 not in rest_map:
            if pg_name_re.match(rest1):
                assert len(rest) == 1
                rest = rest[0]
                pg_name = rest1
                if rest == 'deep-scrub ok':
                    # assert pg_name not in deep_ends, pg_name
                    deep_ends[pg_name] = (date, tm)
                elif rest == 'scrub ok':
                    # assert pg_name not in scrub_ends, pg_name
                    scrub_ends[pg_name] = (date, tm)
                elif rest == 'scrub starts':
                    # assert pg_name not in scrub_start, pg_name
                    scrub_start[pg_name] = (date, tm)
                elif rest == 'deep-scrub starts':
                    # assert pg_name not in deep_start, pg_name
                    deep_start[pg_name] = (date, tm)
                else:
                    print(f"unknown ext: {rest}")


# print(f"{len(deep_start)} {len(deep_ends)} {len(scrub_start)} {len(scrub_ends)}")


def datetime_to_ts(date, tm):
    dt = datetime.datetime.strptime(f"{date} {tm.split('.')[0]}", "%Y-%m-%d %H:%M:%S")
    return time.mktime(dt.timetuple())

# HEADER = "DATE TIME SRC SERVICE_ADDR NUMBER : CLUSTER [LEVEL]"

per_pool = {}
per_pool_deep = {}

for key in scrub_start:
    if key in scrub_ends:
        dtime = datetime_to_ts(*scrub_ends[key]) - datetime_to_ts(*scrub_start[key])
        pool, _ = key.split(".")
        per_pool.setdefault(pool, []).append(dtime)

for key in deep_start:
    if key in deep_ends:
        dtime = datetime_to_ts(*deep_ends[key]) - datetime_to_ts(*deep_start[key])
        pool, _ = key.split(".")
        per_pool_deep.setdefault(pool, []).append(dtime)

names = {
    0: "rbd",
    1: ".rgw.root",
    2: "default.rgw.control",
    3: "default.rgw.data.root",
    4: "default.rgw.gc",
    5: "default.rgw.log",
    6: "default.rgw.users.uid",
    7: "cinder-backup",
    8: "cinder-volumes",
    9: "ephemeral-vms",
    10: "glance-images",
    11: "default.rgw.buckets.index",
    12: "default.rgw.buckets.data",
    14: "ssd-volumes",
    15: "testbench",
    16: "testrbdbench",
    17: "default.rgw.buckets.index.new",
    18: "default.rgw.buckets.index.new1",
    19: "ssd-rbdbench"
}

print("Deep")
for pool, vals in sorted(per_pool_deep.items()):
    avg = int(sum(vals) / len(vals))
    if avg > 30:
        tm = "{:d}:{:02d}".format(avg // 60, avg % 60)
        mx = int(max(vals))
        tm2 = "{:d}:{:02d}".format(mx // 60, mx % 60)
        print(f"{names[int(pool)]:>40s} {tm:>8s} {tm2:>8s}")

print("Scrub")
for pool, vals in sorted(per_pool.items()):
    avg = int(sum(vals) / len(vals))
    if avg > 30:
        tm = "{:d}:{:02d}".format(avg // 60, avg % 60)
        mx = int(max(vals))
        tm2 = "{:d}:{:02d}".format(mx // 60, mx % 60)
        print(f"{names[int(pool)]:>40s} {tm:>8s} {tm2:>8s}")
