import yaml
import asyncio
import datetime
import typing as tp
from pathlib import Path
from collections import deque


def load_config(config_file='private/config.yaml') -> dict:
    config_file = Path(config_file)
    assert config_file.exists()
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


class RateLimiter:
    """
    Limit the number of tasks per minute
    """
    MINUTE = datetime.timedelta(minutes=1)

    def __init__(self, n_tasks_per_minute: int):
        self.n_tasks_per_minute = n_tasks_per_minute
        self.n_running_tasks = 0
        self.time_queue = deque()  # queue of last requests times

    async def wait_on_rate_limit(self):
        # Wait on rate limit
        while self.n_running_tasks + len(self.time_queue) >= self.n_tasks_per_minute:
            await self._wait()
        self.n_running_tasks += 1

    def finish_task(self):
        # Add current request to the queue
        self.time_queue.append(datetime.datetime.now())
        self.n_running_tasks -= 1

    async def _wait(self):
        # Drop irrelevant requests from the queue
        current_time = datetime.datetime.now()
        while self.time_queue and current_time - self.time_queue[0] > self.MINUTE:
            self.time_queue.popleft()

        # Wait on rate limit
        current_time = datetime.datetime.now()
        if len(self.time_queue) + self.n_running_tasks >= self.n_tasks_per_minute:
            await asyncio.sleep(1)


async def limited_gather(*tasks: tp.Awaitable, rate_limiter: RateLimiter):
    """
    asyncio.gather with rate limit
    """
    async def worker(task: tp.Awaitable):
        await rate_limiter.wait_on_rate_limit()
        result = await task
        rate_limiter.finish_task()
        return result
    tasks = [worker(task) for task in tasks]
    return await asyncio.gather(*tasks)


if __name__ == '__main__':
    print(load_config())
