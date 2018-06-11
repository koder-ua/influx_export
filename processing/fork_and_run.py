import os
import time
import fcntl
import selectors
import multiprocessing
from typing import List, Tuple, Callable, Iterable


F_SETPIPE_SZ = 1031


def fork_and_run(max_jobs: int,
                 client_func: Callable[[int, int, int, int], None],
                 task_size: int,
                 min_sz: int = 32) -> Iterable[Tuple[int, bytes]]:
    max_pipe_sz = min(1024 ** 2, int(open("/proc/sys/fs/pipe-max-size").read()))
    buff_size = max_pipe_sz // 2
    max_jobs = min(max_jobs, multiprocessing.cpu_count())
    rec_per_job = max(task_size // max_jobs, min_sz)
    offsets: List[Tuple[int, int]] = []

    start = 0
    while start < task_size:
        end = min(task_size, start + rec_per_job)
        if end + rec_per_job > task_size:
            end = task_size
        offsets.append((start, end))
        start = end

    active_pipes = 0
    sel = selectors.DefaultSelector()

    for idx, (start, stop) in enumerate(offsets):
        read_fd, write_fd = os.pipe()
        fcntl.fcntl(write_fd, F_SETPIPE_SZ, max_pipe_sz)
        if os.fork() == 0:
            try:
                time.sleep(0.0)
                os.close(read_fd)
                os.sched_setaffinity(0, [idx])
                client_func(write_fd, start, stop - start, buff_size)
            finally:
                os.close(write_fd)
            os._exit(0)
        else:
            os.close(write_fd)
            flag = fcntl.fcntl(read_fd, fcntl.F_GETFD)
            fcntl.fcntl(read_fd, fcntl.F_SETFL, flag | os.O_NONBLOCK)
            active_pipes += 1
            sel.register(read_fd, selectors.EVENT_READ)

    while active_pipes:
        for key, mask in sel.select():
            assert mask == selectors.EVENT_READ
            data = os.read(key.fileobj, max_pipe_sz)
            if len(data) == 0:
                active_pipes -= 1
                sel.unregister(key.fileobj)
                os.close(key.fileobj)
            else:
                yield key.fileobj, data

