import io
import sys
import pstats
import argparse
import cProfile
import multiprocessing
from pathlib import Path
from typing import Any, List, Optional


from .config import parse_config
from .tags import load_dev_tags
from .preprocess import preprocess


def parse_args(args: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', action="store_true")

    subparsers = parser.add_subparsers(dest='subparser_name')
    preprocess_parser = subparsers.add_parser('preprocess', help='Preprocess raw data from cluster')
    preprocess_parser.add_argument('-d', '--dev-classes', metavar="FILE", nargs='+')
    preprocess_parser.add_argument('-j', '--max-jobs', metavar="JOBS", type=int, default=multiprocessing.cpu_count())
    preprocess_parser.add_argument('--ctypes', action='store_true', help="Use C++ implementation of data process funcs")
    preprocess_parser.add_argument('-z', '--tz-offset', type=int, default=7200,
                                   help="workaround for timezone shift problem. Second diffe beetween cluster " +
                                        "and current timezone")
    preprocess_parser.add_argument('-s', '--sync-config', metavar="SYNC_CONFIG_FILE")
    preprocess_parser.add_argument('--hdf', metavar="HDF_FILE")
    preprocess_parser.add_argument('raw_src', metavar='RAW_SRC', nargs='*')

    analyze_parser = subparsers.add_parser('analyze', help='Run analisys over data')
    analyze_parser.add_argument('-c', '--cfg', default=str(Path(__file__).parent / "config.yaml"))
    analyze_parser.add_argument('hdf-storage', metavar="FILE")

    plot_ceph_io = subparsers.add_parser('plot_ceph_io', help='Calculate and plot total io on ceph data disks')
    plot_ceph_io.add_argument('hdf-storage', metavar="FILE")

    return parser.parse_args(args[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)

    pr: Optional[cProfile.Profile] = None
    if opts.profile:
        pr = cProfile.Profile()
        pr.enable()

    if opts.subparser_name == 'preprocess':
        cfg = parse_config(opts.sync_config)
        tags = load_dev_tags(*opts.dev_classes)
        preprocess(opts, cfg, tags)
    elif opts.subparser_name == 'plot_ceph_io':
        pass
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
