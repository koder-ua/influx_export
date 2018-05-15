import re
import enum
import json
import pathlib
from typing import List, Dict, Tuple


import yaml
from dataclasses import dataclass


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


def load_osd_tree(osd_three_js_path: pathlib.Path) -> Tuple[List[int], Dict[int, CrushItem]]:
    nodes: Dict[int, CrushItem] = {}
    roots: List[int] = []

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
                roots.append(node.id)

        nodes[node.id] = node

    return roots, nodes


def load_ceph_disk_list(dir_name: pathlib.Path, osds: Dict[int, OSD]):
    for fobj in dir_name.iterdir():
        j4: Dict[str, str] = {}
        osd4dev: Dict[str, OSD] = {}
        for dinfo in json.load(fobj.open()):
            dev = dinfo['path']
            for partition in dinfo.get('partitions', []):
                if partition['type'] == 'data' and partition.get('state') == "active":
                    osd = osds[int(partition['whoami'])]
                    osd.data_dev = dev
                    osd4dev[partition["path"]] = osd
                    osd.node = fobj.name
                elif partition['type'] == 'journal':
                    if "journal_for" in partition:
                        j4[partition["journal_for"]] = dev

        for data_part, j_dev in j4.items():
            osd4dev[data_part].journal_dev = j_dev


RE = "/var/lib/ceph/osd/ceph-(?P<osd_id>\d+)/journal -> (?P<dev_name>/dev/sd.)\d"


def load_ls_osd_dirs(dir_name: pathlib.Path, osds: Dict[int, OSD]):
    not_found_osd = 0
    for fobj in dir_name.iterdir():
        for mobj in re.finditer(RE, fobj.open().read()):
            osd_id = int(mobj.group("osd_id"))
            if osd_id in osds:
                osds[osd_id].journal_dev = mobj.group("dev_name")
            else:
                not_found_osd += 1
    if not_found_osd:
        print(f"Found {not_found_osd} dirs from non-existing anymore osd's")


def find_roots(roots: List[int], nodes: Dict[int, CrushItem]) -> List[OSD]:
    osds: List[OSD] = []

    def _propagate_roots(root: str, node: CrushItem):
        if isinstance(node, OSD):
            node.roots.append(root)
            osds.append(node)
        else:
            for ch_node_id in node.children_ids:
                _propagate_roots(root, nodes[ch_node_id])

    for root_id in roots:
        _propagate_roots(nodes[root_id].name, nodes[root_id])

    return osds


base = pathlib.Path("~/workspace/RIL_data/cluster_info").expanduser()
roots, units = load_osd_tree(base / "cephosdtree.js")
osds = find_roots(roots, units)
osds_mp = {osd.id: osd for osd in osds}

load_ls_osd_dirs(base / "journals_ls", osds_mp)
load_ceph_disk_list(base / "ceph_disk_list_js", osds_mp)

class DiskTypes(enum.Enum):
    data = 1
    journal = 2


active_osds = [osd for osd in osds if osd.node is not None]
dev_types: Dict[Tuple[str, str], Tuple[str, DiskTypes]] = {}
for osd in sorted(active_osds, key=lambda x: (x.node, x.id)):
    assert len(osd.roots) == 1
    key = (osd.node, osd.data_dev)
    val = (osd.roots[0], DiskTypes.data)
    if key in dev_types:
        assert dev_types[key] == val
    else:
        dev_types[key] = val

    key = (osd.node, osd.journal_dev)
    val = (osd.roots[0], DiskTypes.journal)
    if key in dev_types:
        assert dev_types[key] == val
    else:
        dev_types[key] = val


mp: Dict[str, Dict[str, Dict[str, str]]] = {}
for (node, dev), (root, tp) in sorted(dev_types.items()):
    mp.setdefault(node, {})[dev] = {"type": "ceph_disk",
                                    "crush_root": root,
                                    "ceph_disk_type": tp.name}

print(yaml.dump(mp, indent=4))



