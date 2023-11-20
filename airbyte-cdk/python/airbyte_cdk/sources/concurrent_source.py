#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import asyncio
from typing import Any, Dict, List, Tuple

PartitionType = Tuple[int, int]
RecordType = Dict[str, Any]


async def generate_partition(partition: int) -> PartitionType:
    partition_size = 10
    return partition * partition_size, (partition + 1) * partition_size


async def read_partition(partition: PartitionType) -> List[RecordType]:
    return [{"record": i} for i in range(*partition)]


async def read_stream() -> List[RecordType]:
    n_partitions = 3
    pending = {asyncio.create_task(generate_partition(p)) for p in range(n_partitions)}
    results = []

    while True:
        done, pending = await asyncio.wait(pending)

        for task in done:
            result = task.result()
            if isinstance(result, tuple):
                pending.add(asyncio.create_task(read_partition(result)))
            elif isinstance(result, list):
                results.extend(result)
            else:
                raise Exception(f"unrecognized {result}")
        if not pending:
            return results


async def read_source() -> List[RecordType]:
    records = []
    for stream in range(1):
        records.extend(await read_stream())
    return records


async def entrypoint_read():
    for record in await read_source():
        print(record)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(entrypoint_read())
