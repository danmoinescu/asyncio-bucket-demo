#!/usr/bin/python3

import asyncio
import random


async def worker(val: int) -> int:
    print(f"starting worker {val}")
    sleep_time = random.random()*10
    await asyncio.sleep(sleep_time)
    return val, sleep_time


async def bucket_loop(max_items: int, bucket_size: int) -> None:
    loop = asyncio.get_running_loop()
    tasks = set()
    results = {}

    for i in range(1, bucket_size+1):
        tasks.add(loop.create_task(worker(i)))

    while tasks:
        done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
        #print(f"Another {len(done)} task(s) completed")
        for task in done:
            result = task.result()
            results[result[0]] = result[1]
            print(f"worker done: {result}, {len(tasks)} task(s) remaining")
            tasks.remove(task)
            if i < max_items:
                i += 1
                tasks.add(loop.create_task(worker(i)))
    return results

if __name__ == "__main__":
    results = asyncio.run(bucket_loop(17, 5))
    for val, t in results.items():
        print(f"{val}: {t:.2f}")

