import re
import os
import sys
import abc
import enum
import json
import pathlib
import argparse
import tempfile
import contextlib
import subprocess
import collections
from typing import List, Dict, Tuple, Any, Union, Set


import yaml
from dataclasses import dataclass, field


@dataclass
class CrushItem:
    id: int
    children_ids: List[int]
    name: str


@dataclass
class OSD(CrushItem):
    node: str
    journal_dev: str
    data_dev: str
    roots: List[str]
    data_part: str = None
    journal_part: str = None


class DiskTypes(enum.Enum):
    ceph_data = 1
    ceph_journal = 2


@dataclass
class DevInfo:
    tags: Set[str] = field(default_factory=set)
    attrs: Dict[str, Union[str, int]] = field(default_factory=dict)

    def dct(self) -> Dict[str, Union[str, int, List[str]]]:
        assert "__tags__" not in self.attrs
        res = {"__tags__": list(self.tags)}
        res.update(self.attrs)
        return res


class DevInformer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_devs_info(self, devs: Dict[str, Dict[str, DevInfo]]):
        pass


class CephClusterInfo(DevInformer):
    journal_rr = re.compile(r"/var/lib/ceph/osd/ceph-(?P<osd_id>\d+)/journal " +
                            r"-> (?P<dev_name>/dev/sd.)(?P<part_num>\d?)")

    def __init__(self, base: pathlib.Path) -> None:
        self.base = base
        self.nodes: Dict[int, CrushItem] = {}
        self.roots: List[int] = []
        self.active_osds: List[OSD] = []

        self.load_osd_tree()
        self.find_active_osds_and_roots()

        self.osds_mp = {osd.id: osd for osd in self.active_osds}

        self.load_ls_osd_dirs()
        self.load_ceph_disk_list()
        # active_osds = [osd for osd in osds if osd.node is not None]

    def load_osd_tree(self):
        osd_three_js_path = self.base / "cephosdtree.js"

        for node_info in json.load(osd_three_js_path.open())["nodes"]:
            if node_info["type"] == 'osd':
                node = OSD(children_ids=[], id=int(node_info["id"]), name=node_info["name"],
                           node=None, journal_dev=None, data_dev=None, roots=[])
                assert node.id >= 0
            else:
                node = CrushItem(id=int(node_info["id"]),
                                 children_ids=node_info["children"],
                                 name=node_info["name"])
                assert node.id < 0
                if node_info["type"] == 'root':
                    self.roots.append(node.id)

            self.nodes[node.id] = node

    def find_active_osds_and_roots(self):
        def _propagate_roots(root: str, node: CrushItem):
            if isinstance(node, OSD):
                node.roots.append(root)
                self.active_osds.append(node)
            else:
                for ch_node_id in node.children_ids:
                    _propagate_roots(root, self.nodes[ch_node_id])

        for root_id in self.roots:
            _propagate_roots(self.nodes[root_id].name, self.nodes[root_id])

    def load_ceph_disk_list(self):
        for fobj in (self.base / "ceph_disk_list_js").iterdir():
            j4: Dict[str, str] = {}
            osd4dev: Dict[str, OSD] = {}
            for dinfo in json.load(fobj.open()):
                dev = dinfo['path']
                for partition in dinfo.get('partitions', []):
                    if partition['type'] == 'data' and partition.get('state') == "active":
                        osd = self.osds_mp[int(partition['whoami'])]
                        osd.data_dev = dev
                        osd.data_part = partition["path"]
                        osd4dev[partition["path"]] = osd
                        osd.node = fobj.name
                    elif partition['type'] == 'journal':
                        if "journal_for" in partition:
                            j4[partition["journal_for"]] = dev, partition['path']

            for data_part, j_dev in j4.items():
                osd4dev[data_part].journal_dev, osd4dev[data_part].journal_part = j_dev

    def load_ls_osd_dirs(self):
        dir_name = self.base / "journals_ls"
        not_found_osd = 0
        for fobj in dir_name.iterdir():
            for mobj in self.journal_rr.finditer(fobj.open().read()):
                osd_id = int(mobj.group("osd_id"))
                if osd_id in self.osds_mp:
                    self.osds_mp[osd_id].journal_dev = mobj.group("dev_name")
                    self.osds_mp[osd_id].journal_part = mobj.group("dev_name") + mobj.group("part_num")
                else:
                    not_found_osd += 1
        if not_found_osd:
            print(f"Found {not_found_osd} dirs from non-existing anymore osd's")

    def update_devs_info(self, devs: Dict[str, Dict[str, DevInfo]]):
        for osd in self.active_osds:
            assert len(osd.roots) == 1

            for dev in {osd.journal_part, osd.journal_dev}:
                jinfo = devs[osd.node][dev]
                jinfo.tags.add("ceph_journal")
                jinfo.attrs["osd_id"] = osd.id
                jinfo.attrs["crush_root"] = osd.roots[0]

            for dev in {osd.data_part, osd.data_dev}:
                dinfo = devs[osd.node][dev]
                dinfo.tags.add("ceph_data")
                dinfo.attrs["osd_id"] = osd.id
                dinfo.attrs["crush_root"] = osd.roots[0]


class CephInfluxProcessorConfigGenerator:
    def __init__(self, cluster_info: CephClusterInfo) -> None:
        self.cluster_info = cluster_info

    def generate_ceph_devices(self):
        dev_types: Dict[Tuple[str, str], Tuple[str, DiskTypes]] = {}
        for osd in sorted(self.cluster_info.active_osds, key=lambda x: (x.node, x.id)):
            assert len(osd.roots) == 1
            key = (osd.node, osd.data_dev)
            val = (osd.roots[0], DiskTypes.ceph_data)
            if key in dev_types:
                assert dev_types[key] == val
            else:
                dev_types[key] = val

            key = (osd.node, osd.journal_dev)
            val = (osd.roots[0], DiskTypes.ceph_journal)
            if key in dev_types:
                assert dev_types[key] == val
            else:
                dev_types[key] = val


        mp: Dict[str, Dict[str, Dict[str, str]]] = {}
        for (node, dev), (root, tp) in sorted(dev_types.items()):
            host_mp = mp.setdefault(f"host={node}", {})
            dev_name = pathlib.Path(dev).name
            host_mp[f"name={dev_name}"] = {"dev_type": f"ceph_{tp.name}", "crush_root": root}

        print(yaml.dump({"diskio_(write|read)_bytes": mp}, indent=4))

    def generate_ceph_partitions(self):
        dev_types: Dict[Tuple[str, str], Tuple[str, DiskTypes]] = {}
        mp: Dict[str, Dict[str, Dict[str, str]]] = {}

        for osd in sorted(self.cluster_info.active_osds, key=lambda x: (x.node, x.id)):
            assert len(osd.roots) == 1
            key = (osd.node, osd.data_part)
            val = (osd.roots[0], DiskTypes.ceph_data)
            if key in dev_types:
                assert dev_types[key] == val
            else:
                dev_types[key] = val

            key = (osd.node, osd.journal_dev)
            val = (osd.roots[0], DiskTypes.ceph_journal)
            if key in dev_types:
                assert dev_types[key] == val
            else:
                dev_types[key] = val

        for (node, dev), (root, tp) in sorted(dev_types.items()):
            if tp.name == 'data':
                host_mp = mp.setdefault(f"host={node}", {})
                dev_name = pathlib.Path(dev).name
                host_mp[f"device={dev_name}"] = {"dev_type": f"ceph_{tp.name}", "crush_root": root}
        print(yaml.dump({"disk_used_percent": mp}, indent=4))


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=['model', 'sync'])
    parser.add_argument("cluster_archive")
    return parser.parse_args(argv)


def main(argv: List[str]):
    opts = parse_args(argv[1:])
    data_path = pathlib.Path(opts.cluster_archive).expanduser()

    if not data_path.exists():
        raise FileNotFoundError(f"data path {opts.cluster_archive} dont' found")

    if data_path.is_file():
        with tempfile.TemporaryDirectory() as decompressed_path:
            with open(os.devnull, 'w') as devnull:
                subprocess.check_call(["tar",  "-C", data_path, "-axvf",  decompressed_path], stdout=devnull)
            ceph_info = CephClusterInfo(pathlib.Path(decompressed_path))
    else:
        ceph_info = CephClusterInfo(data_path)

    data: Dict[str, Dict[str, DevInfo]] = collections.defaultdict(lambda: collections.defaultdict(DevInfo))
    ceph_info.update_devs_info(data)

    if opts.action == 'model':
        res = {}
        for name, val in data.items():
            res[name] = {dname: info.dct() for dname, info in val.items()}

        print(yaml.dump(res, indent=4, Dumper=yaml.SafeDumper))
    elif opts.action == 'sync':
        # is_partition = r"(?P:[hsv]d[a-z]+\d+|nvme\d+n\d+p\d+)$"
        all = set()
        for node, node_info in data.items():
            for device, dev_info in node_info.items():
                if "ceph_data" in dev_info.tags or "ceph_journal" in dev_info.tags:
                    all.add(f"+ disk_ops_write,device={device},hostname={node}")

        for selector in sorted(all):
            print(selector)


if __name__ == "__main__":
    exit(main(sys.argv))


#
# generate_ceph_devices()
# generate_ceph_partitions()
