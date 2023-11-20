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


async def read_stream(stream_name: str, queue: Queue) -> None:
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


async def read_all_streams(queue):
    tasks = [asyncio.create_task(read_stream(f"stream_{stream}", queue)) for stream in range(n_streams)]
    await asyncio.gather(*tasks)


class SourceReader(Iterator[T]):
    def __init__(self):
        self.queue = Queue()
        self.sentinel_count = 0
        Thread(target=lambda: asyncio.run(read_all_streams(self.queue))).start()

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


def read_source() -> Iterator[RecordType]:
    return SourceReader()


def entrypoint_read():
    for record in read_source():
        print(record)


if __name__ == "__main__":
    entrypoint_read()
