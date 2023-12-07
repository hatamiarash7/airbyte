#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import asyncio
from queue import Queue
from threading import Thread
from typing import Any, Dict, Iterator, Tuple, TypeVar

PartitionType = Tuple[int, int]
RecordType = Dict[str, Any]
T = TypeVar("T")

n_streams = 3


async def generate_partition(partition: int) -> PartitionType:
    partition_size = 10
    return partition * partition_size, (partition + 1) * partition_size


async def read_partition(stream_name: str, partition: PartitionType, queue: Queue) -> None:
    for i in range(*partition):
        queue.put({f"{stream_name}_record": i})
        await asyncio.sleep(0.001)  # simulate I/O wait time; demonstrates that streams do get interleaved


class SourceReader(Iterator[T]):
    def __init__(self, queue, reader_fn, *args):
        self.queue = queue
        self.sentinel_count = 0
        Thread(target=lambda: asyncio.run(reader_fn(*args))).start()

    def __next__(self) -> T:
        item = self.queue.get()
        if "__done_" in item:
            self.sentinel_count += 1
            if self.sentinel_count == n_streams:
                raise StopIteration
            else:
                return self.__next__()
        else:
            return item


class Source:
    def __init__(self):
        self.queue = Queue()

    def streams(self):
        return [Stream(i) for i in range(n_streams)]

    def read(self) -> Iterator[RecordType]:
        for record in SourceReader(self.queue, self._read_streams):
            yield record

    async def _read_streams(self):
        tasks = [asyncio.create_task(s.read_stream(s.name, self.queue)) for s in self.streams()]
        await asyncio.gather(*tasks)


class Stream:
    def __init__(self, name):
        self.name = f"stream_{name}"

    async def read_stream(self, stream_name: str, queue: Queue) -> None:
        n_partitions = 3
        pending = {asyncio.create_task(generate_partition(p)) for p in range(n_partitions)}

        while True:
            done, pending = await asyncio.wait(pending)

            for task in done:
                result = task.result()
                if isinstance(result, tuple):
                    pending.add(asyncio.create_task(read_partition(stream_name, result, queue)))
                elif result is None:
                    pass
                else:
                    raise Exception(f"unrecognized {result}")
            if not pending:
                queue.put(f"__done_{stream_name}")
                break


def entrypoint_read():
    source = Source()
    for record in source.read():
        print(record)


if __name__ == "__main__":
    entrypoint_read()
