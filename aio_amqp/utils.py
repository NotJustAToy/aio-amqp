# Copyright 2020 Not Just A Toy Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio as aio
import typing as ty
from collections import defaultdict
from contextlib import asynccontextmanager
from functools import partial

__all__ = (
    'chunk_iter',
    'MultiLock',
    'ChannelIds',
)


def chunk_iter(data: bytes, chunk_size: ty.Optional[int] = None) -> ty.Generator[bytes, None, None]:
    if chunk_size is None:
        chunk_size = len(data)

    a, b = 0, chunk_size
    while True:
        chunk = data[a:b]
        a, b = b, b + chunk_size

        if chunk:
            yield chunk

        else:
            return


class MultiLock:

    def __init__(self, loop: ty.Optional[aio.AbstractEventLoop] = None) -> None:
        loop = loop or aio.get_event_loop()
        self._locks: ty.DefaultDict[ty.Hashable, aio.Lock] = defaultdict(partial(aio.Lock, loop=loop))
        self._wait_counts: ty.DefaultDict[ty.Hashable, int] = defaultdict(int)

    async def acquire(self, key: ty.Hashable) -> bool:
        self._wait_counts[key] += 1
        return await self._locks[key].acquire()

    def locked(self, key: ty.Hashable) -> bool:
        return self._locks[key].locked()

    def release(self, key: ty.Hashable) -> None:
        self._locks[key].release()
        self._wait_counts[key] -= 1
        if self._wait_counts[key] < 1:
            del self._locks[key]
            del self._wait_counts[key]

    @asynccontextmanager
    async def lock(self, key: ty.Hashable) -> ty.AsyncGenerator[None, None]:
        await self.acquire(key)
        try:
            yield
        finally:
            self.release(key)


class ChannelIds:

    def __init__(self) -> None:
        self._last_channel_id = None
        self._free_channel_ids: ty.Set[int] = set()
        self._used_channel_ids: ty.Set[int] = set()

    def get_free(self) -> int:
        if self._last_channel_id is None:
            channel_id = 1

        elif self._free_channel_ids:
            channel_id = self._free_channel_ids.pop()

        else:
            while True:
                channel_id = self._last_channel_id + 1
                if channel_id not in self._used_channel_ids:
                    break

        self._used_channel_ids.add(channel_id)
        return channel_id

    def use(self, channel_id: int) -> None:
        self._free_channel_ids.discard(channel_id)
        self._used_channel_ids.add(channel_id)

    def free(self, channel_id: int) -> None:
        self._free_channel_ids.add(channel_id)
        self._used_channel_ids.discard(channel_id)

    def bad(self, channel_id: int) -> None:
        self._free_channel_ids.discard(channel_id)
        self._used_channel_ids.discard(channel_id)

    def reset(self) -> None:
        self._last_channel_id = None
        self._free_channel_ids.clear()
        self._used_channel_ids.clear()
