#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import asyncio
from queue import Queue
from typing import Any, Dict, List, Tuple

PartitionType = Tuple[int, int]
RecordType = Dict[str, Any]
n_streams = 3


async def generate_partition(partition: int) -> PartitionType:
    partition_size = 10
    return partition * partition_size, (partition + 1) * partition_size


async def read_partition(partition: PartitionType) -> List[RecordType]:
    return [{"record": i} for i in range(*partition)]


async def read_stream(stream_name: str, queue: Queue) -> None:
    n_partitions = 3
    pending = {asyncio.create_task(generate_partition(p)) for p in range(n_partitions)}

    while True:
        done, pending = await asyncio.wait(pending)

        for task in done:
            result = task.result()
            if isinstance(result, tuple):
                pending.add(asyncio.create_task(read_partition(result)))
            elif isinstance(result, list):
                queue.put(f"{stream_name}, {result}")
            else:
                raise Exception(f"unrecognized {result}")
        if not pending:
            queue.put(f"__done_{stream_name}")
            break


async def read_all_streams(queue):
    for stream in range(n_streams):
        await read_stream(f"stream_{stream}", queue)


def read_source() -> List[RecordType]:
    queue = Queue()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(read_all_streams(queue))
    sentinel_count = 0

    while True:
        item = queue.get()
        if "__done_" in item:
            sentinel_count += 1
            if sentinel_count == n_streams:
                return
        else:
            yield item


def entrypoint_read():
    for record in read_source():
        print(record)


if __name__ == "__main__":
    entrypoint_read()
