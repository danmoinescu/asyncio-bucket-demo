#!/usr/bin/python3 -Wall

import asyncio
import functools
import random
from collections.abc import Awaitable, Iterable
from typing import Any


def log_when_starts(func):
    """ A decorator that prints details (name, arguments)
        when the decorated function is called.
    """
    @functools.wraps(func)
    def log_wrapper(*args, **kwargs):
        all_args = [repr(a) for a in args]
        all_args += [f"{name}={repr(val)}" for name, val in kwargs.items()]
        all_args_str = ", ".join(all_args)
        print(f"{func.__name__}({all_args_str}) starting")
        return func(*args, **kwargs)
    return log_wrapper


@log_when_starts
async def worker(val: int) -> int:
    sleep_time = random.random()*10
    await asyncio.sleep(sleep_time)
    return val, sleep_time


async def bucket_loop(
        async_job: Awaitable[[...], Any],
        items: Iterable[Any],
        bucket_size: int) -> list[Any]:
    loop = asyncio.get_running_loop()
    tasks = set()
    results = {}
    item_iterator = iter(items)
    have_more_items = True

    for i in range(0, bucket_size):
        if have_more_items:
            try:
                tasks.add(loop.create_task(
                            async_job(next(item_iterator))))
            except StopIteration:
                have_more_items = False

    while tasks:
        done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)
        #print(f"Another {len(done)} task(s) completed")
        for task in done:
            result = task.result()
            results[result[0]] = result[1]
            print(f"task completed: {result}, {len(tasks)} task(s) remaining")
            tasks.remove(task)
            if have_more_items:
                try:
                    tasks.add(loop.create_task(
                                async_job(next(item_iterator))))
                except StopIteration:
                    have_more_items = False
    return results

if __name__ == "__main__":
    results = asyncio.run(
            bucket_loop(
                async_job=worker, items=range(1, 14), bucket_size=5))
    print("Results:")
    for val, t in results.items():
        print(f"{val}: {t:.2f}")

