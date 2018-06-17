import datetime
from typing import Dict, Optional

import numpy
from dataclasses import dataclass, field
from pandas import Series

metric2units = {
    "cpu_usage_guest": "s",
    "cpu_usage_iowait": "s",
    "cpu_usage_system": "s",
    "cpu_usage_user": "s",
    "disk_used_percent": None,
    "diskio_iops_in_progress": None,
    "diskio_read_bytes": "B",
    "diskio_write_bytes": "B",
    "libvirt_domain_block_stats_read_bytes_total": "B",
    "libvirt_domain_block_stats_read_requests_total": None,
    "libvirt_domain_block_stats_write_bytes_total": "B",
    "libvirt_domain_block_stats_write_requests_total": None,
    "libvirt_domain_info_cpu_time_seconds_total": "s",
    "libvirt_domain_info_maximum_memory_bytes": "B",
    "libvirt_domain_info_memory_usage_bytes": "B",
    "libvirt_domain_info_virtual_cpus": None,
    "net_bytes_recv": "B",
    "net_bytes_sent": "B",
}


metric2devices_attrs = {
    "disk_used_percent": "device",
    "diskio_iops_in_progress": "name",
    "diskio_read_bytes": "name",
    "diskio_write_bytes": "name",
    "net_bytes_recv": "interface",
    "net_bytes_sent": "interface",
}


def merge_unit_dev_host(metric: str, attrs: Dict[str, str]) -> Dict[str, str]:
    if metric.startswith("libvirt_domain"):
        host = None
        device = attrs['domain']
    else:
        host = attrs['host']
        device = "cpu" if metric.startswith("cpu_usage") else attrs[metric2devices_attrs[metric]]

    units = metric2units[metric]
    new_attrs = attrs.copy()
    unit_dev_host = {'host': host, 'device': device, 'units': units}
    for name, val in unit_dev_host.items():
        assert attrs.get(name) in (val, None, ''), f"attrs[{name}] == {attrs[name]} != {val}"
    new_attrs.update(unit_dev_host)
    return new_attrs


@dataclass
class Serie:
    name: str
    metric: str
    attrs: Dict[str, str]
    times: Optional[numpy.ndarray] = None
    vals: numpy.ndarray = field(default_factory=lambda: numpy.array([]))
    offset: Optional[int] = None
    size: Optional[int] = None
    origin: Optional[numpy.ndarray] = None

    def __str__(self) -> str:
        return f"Serie(name='{self.name}', sz={len(self.vals)})"

    @property
    def units(self) -> str:
        return self.attrs['units']

    @property
    def host(self) -> str:
        return self.attrs['host']

    @property
    def device(self) -> str:
        return self.attrs['device']

    @property
    def pandas(self) -> Series:
        return Series(data=self.vals,
                      index=[datetime.datetime.fromtimestamp(vl) for vl in self.times])

    @property
    def compaction_type(self) -> numpy.dtype:
        return numpy.dtype(numpy.uint16)