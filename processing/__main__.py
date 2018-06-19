import io
import sys
import pstats
import argparse
import cProfile
import multiprocessing
from pathlib import Path
from typing import Any, List, Optional


import yaml


from .config import parse_config
from .preprocess import do_import, symulate_import
from .process import process


def load_config_file(opts: Any):
    cpath = Path(opts.config).expanduser()
    if cpath.exists():
        config = yaml.load(cpath.open())
        assert not set(config).intersection(opts.__dict__)
        opts.__dict__.update(config)


ceph_cmd_subparsers = ['plot_ceph_io', 'plot_ceph_qd', 'plot_ceph_usage', 'plot_ceph_cpu_user', 'plot_ceph_cpu_per_mb']
bneck_subparsers = ['cpu_bottleneck', 'disk_qd_bottleneck', 'network_bottleneck']


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default="~/.mira-ml/config.yaml")
    parser.add_argument('--profile', action="store_true")

    subparsers = parser.add_subparsers(dest='subparser_name')

    import_parser = subparsers.add_parser('import', help='Import raw data into storage')
    import_parser.add_argument('-c', '--cluster', required=True, help="Cluster cluster name")
    import_parser.add_argument('--simulate', action='store_true',
                               help="Don't import data into storage, just process them")
    import_parser.add_argument('-j', '--max-jobs', metavar="JOBS",
                               type=int, default=multiprocessing.cpu_count(), help="Import threads count")
    import_parser.add_argument('--no-ctypes', action='store_true',
                               help="Don't useC++ implementation of data process funcs")
    import_parser.add_argument('--compact', action='store_true',
                               help="Compact all data to 16 bit per measurement")
    import_parser.add_argument('-s', '--sync-config', metavar="SYNC_CONFIG_FILE", required=True)
    import_parser.add_argument('raw_src', metavar='RAW_SRC', help="File to import")

    for name in ceph_cmd_subparsers:
        cparser = subparsers.add_parser(name)
        cparser.add_argument('-r', '--crush-root', default='default', help="Crush root to analyze")
        cparser.add_argument('-c', '--cluster', required=True, help="Cluster cluster name")
        cparser.add_argument('-f', '--force-update', action='store_true', help="Recalculate metric")

    for name in bneck_subparsers:
        cparser = subparsers.add_parser(name)
        cparser.add_argument('-c', '--cluster', required=True, help="Cluster cluster name")

    return parser.parse_args(args[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    load_config_file(opts)

    pr: Optional[cProfile.Profile] = None
    if opts.profile:
        pr = cProfile.Profile()
        pr.enable()

    if opts.subparser_name == 'import':
        cfg = parse_config(opts.sync_config)
        if opts.simulate:
            symulate_import(opts, cfg)
        else:
            do_import(opts, cfg)
    elif opts.subparser_name in (ceph_cmd_subparsers + bneck_subparsers):
        process(opts.subparser_name, opts)
    else:
        print(f"Action {opts.subparser_name} is not supported")
        return 1

    if pr is not None:
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats(10000)
        print(s.getvalue())

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
