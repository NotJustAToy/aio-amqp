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
import collections
import dataclasses
import datetime
import time
import typing as ty
import uuid
from ssl import SSLContext

from pamqp.body import ContentBody
from pamqp.exceptions import UnmarshalingException
from pamqp.frame import frame_parts, marshal, unmarshal
from pamqp.header import ContentHeader, ProtocolHeader
from pamqp.heartbeat import Heartbeat
from pamqp.specification import (
    Basic,
    Channel,
    Confirm,
    Connection,
    Exchange,
    Frame,
    FRAME_MIN_SIZE,
    FRAME_MAX_SIZE,
    Tx,
    Queue
)

from .constants import *
from .exceptions import *
from .utils import *
from .version import *

__all__ = (
    'T_Arguments',
    'T_Properties',
    'ReceivedMessage',
    'DeliveredMessage',
    'ReturnedMessage',
    'ConnectionOpenResult',
    'ChannelOpenResult',
    'QueueDeclareResult',
    'QueueDeleteResult',
    'QueuePurgeResult',
    'BasicConsumeResult',
    'BasicCancelResult',
    'Client',
)

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


T_AnyFrame = ty.Union[
    ContentBody,
    ContentHeader,
    Frame,
    Heartbeat,
    ProtocolHeader
]

T_Arguments = ty.Mapping[str, ty.Any]
T_Properties = ty.Mapping[str, ty.Any]


class ReceivedMessage(ty.NamedTuple):
    body: bytes
    channel_id: int
    delivery_tag: int
    exchange: str
    properties: T_Properties
    redelivered: bool
    routing_key: str
    timestamp: datetime.datetime


class DeliveredMessage(ty.NamedTuple):
    body: bytes
    channel_id: int
    consumer_tag: str
    delivery_tag: int
    exchange: str
    properties: T_Properties
    redelivered: bool
    routing_key: str
    timestamp: datetime.datetime


class ReturnedMessage(ty.NamedTuple):
    body: bytes
    channel_id: int
    exchange: str
    properties: T_Properties
    reply_code: int
    reply_text: str
    routing_key: str
    timestamp: datetime.datetime


class ConnectionOpenResult(ty.NamedTuple):
    close_reason: aio.Future


class ChannelOpenResult(ty.NamedTuple):
    channel_id: int
    close_reason: aio.Future


class QueueDeclareResult(ty.NamedTuple):
    queue: str
    message_count: int
    consumer_count: int


class QueueDeleteResult(ty.NamedTuple):
    message_count: int


class QueuePurgeResult(ty.NamedTuple):
    message_count: int


class BasicConsumeResult(ty.NamedTuple):
    consumer_tag: str
    cancel_reason: aio.Future


class BasicCancelResult(ty.NamedTuple):
    consumer_tag: str


def delivery_tags() -> ty.Generator[int, None, None]:
    delivery_tag = 1
    while True:
        yield delivery_tag
        delivery_tag += 1


class Client:

    @dataclasses.dataclass
    class ChannelData:
        consumer_tags: ty.Set[str] = dataclasses.field(default_factory=set)
        delivery_tags: ty.Generator[int, None, None] = dataclasses.field(default_factory=delivery_tags)
        unconfirmed_delivery_tags: ty.Set[int] = dataclasses.field(default_factory=set)
        publisher_confirm: bool = False

    def __init__(
            self,
            channel_max: ty.Optional[int] = None,
            frame_max: ty.Optional[int] = None,
            heartbeat: ty.Optional[int] = None,
            client_properties: ty.Optional[T_Properties] = None,
            heartbeat_interval_multiplier: float = 0.5,
            heartbeat_grace_multiplier: float = 2,
            loop: ty.Optional[aio.AbstractEventLoop] = None
    ) -> None:
        if channel_max is not None and channel_max <= 0:
            raise ValueError("Channel max must be > 0")

        self._channel_max = channel_max

        if frame_max is not None and (frame_max < FRAME_MIN_SIZE or frame_max > FRAME_MAX_SIZE):
            raise ValueError("Frame max must be >= %d and <= %d" % (FRAME_MIN_SIZE, FRAME_MAX_SIZE))

        self._frame_max = frame_max

        if heartbeat is not None and heartbeat <= 0:
            raise ValueError("Heartbeat must be > 0")

        self._heartbeat = heartbeat

        if client_properties is None:
            client_properties = {}
        self._client_properties = client_properties

        if heartbeat_interval_multiplier <= 0 or heartbeat_interval_multiplier > 1:
            raise ValueError("Heartbeat interval multiplier must be > 0 and <= 1")
        self._heartbeat_interval_multiplier = heartbeat_interval_multiplier

        if heartbeat_grace_multiplier < 1:
            raise ValueError("Heartbeat grace multiplier must be >= 1")
        self._heartbeat_grace_multiplier = heartbeat_grace_multiplier

        self._loop = loop or aio.get_event_loop()

        self._reader: ty.Optional[aio.StreamReader] = None
        self._writer: ty.Optional[aio.StreamWriter] = None

        self._connection_lock = aio.Lock(loop=self._loop)
        self._channel_lock = MultiLock(loop=self._loop)
        self._drain_lock = aio.Lock(loop=self._loop)

        self._last_receive_time: ty.Optional[float] = None

        self._connection_open = False

        self._channel_ids = ChannelIds()
        self._channels: ty.Dict[int, 'Client.ChannelData'] = {}

        self._server_properties: ty.Optional[T_Properties] = None
        self._tune_channel_max: ty.Optional[int] = None
        self._tune_frame_max: ty.Optional[int] = None
        self._tune_heartbeat: ty.Optional[int] = None

        self._waiters: ty.DefaultDict[ty.Hashable, ty.Set[aio.Future]] = collections.defaultdict(set)
        self._tasks: ty.List[aio.Task] = []

        self._reasons: ty.Dict[ty.Hashable, aio.Future] = {}

        self._consumer_queues: ty.DefaultDict[ty.Hashable, ty.Set[aio.Queue]] = collections.defaultdict(set)

    async def connection_open(
            self,
            host: str,
            port: int = 5672,
            virtual_host: str = '/',
            username: str = 'guest',
            password: str = 'guest',
            ssl: ty.Optional[ty.Union[bool, SSLContext]] = None
    ) -> ConnectionOpenResult:
        if len(host) == 0:
            raise ValueError("Invalid host")

        if port <= 0:
            raise ValueError("Invalid port")

        async with self._connection_lock:
            if self._connection_open:
                raise ConnectionAlreadyOpenError()

            try:
                self._reader, self._writer = await aio.open_connection(
                    host=host, port=port,  loop=self._loop, ssl=ssl)
            except OSError as e:
                raise ConnectFailedError() from e

            try:
                protocol_header = ProtocolHeader(
                    major_version=PROTOCOL_VERSION[0],
                    minor_version=PROTOCOL_VERSION[1],
                    revision=PROTOCOL_VERSION[2]
                )
                await self._send(protocol_header.marshal())

                try:
                    _, _, frame = await self._receive_frame()
                except ConnectionLostError as e:
                    raise ProtocolIncompatibleError() from e

                if not isinstance(frame, Connection.Start):
                    raise UnexpectedFrameReceivedError()

                self._server_properties = frame.server_properties

                mechanisms = frame.mechanisms.split()
                if b'PLAIN' not in mechanisms:
                    raise UnsupportedAuthMechanismsError()

                client_properties = {
                    'platform': PLATFORM,
                    'version': __version__,
                    'capabilities': {
                        'authentication_failure_close': True,
                        'basic.nack': True,
                        'connection.blocked': False,
                        'consumer_cancel_notify': True,
                        'publisher_confirms': True,
                    }
                }
                client_properties.update(self._client_properties)

                connection_start_ok_frame = Connection.StartOk(
                    client_properties,
                    response='\0%s\0%s' % (username, password)
                )
                await self._send_frame(0, connection_start_ok_frame)

                _, _, frame = await self._receive_frame()
                self._raise_if_connection_close_received(frame)

                if not isinstance(frame, Connection.Tune):
                    raise UnexpectedFrameReceivedError()

                self._tune_channel_max = self._negotiate_integer_value(self._channel_max, frame.channel_max)
                self._tune_frame_max = self._negotiate_integer_value(self._frame_max, frame.frame_max)
                self._tune_heartbeat = self._tune_heartbeat_timeout(self._heartbeat, frame.heartbeat)

                connection_tune_ok_frame = Connection.TuneOk(
                    channel_max=self._tune_channel_max,
                    frame_max=self._tune_frame_max,
                    heartbeat=self._tune_heartbeat
                )
                await self._send_frame(0, connection_tune_ok_frame)

                connection_open_frame = Connection.Open(
                    virtual_host=virtual_host
                )
                await self._send_frame(0, connection_open_frame)

                _, _, frame = await self._receive_frame()
                self._raise_if_connection_close_received(frame)

                if not isinstance(frame, Connection.OpenOk):
                    raise UnexpectedFrameReceivedError()

                self._tasks.append(self._loop.create_task(self._receiving_task()))
                self._tasks.append(self._loop.create_task(self._heart_task()))
                self._tasks.append(self._loop.create_task(self._heartbeat_monitor_task()))

                for task in self._tasks:
                    task.add_done_callback(self._task_done_callback)

                self._wakeup('connection_open')
            except Exception:
                self._close_connection()
                raise

            self._connection_open = True

            future = self._loop.create_future()
            self._reasons['connection_close'] = future

            return ConnectionOpenResult(close_reason=future)

    async def connection_close(self) -> None:
        async with self._connection_lock:
            self._raise_if_connection_closed()

            request = Connection.Close(reply_code=REPLY_CODE.SUCCESS)
            try:
                await self._rpc(0, request)
            except Exception:
                self._close_connection()
                raise

            self._close_connection()

    async def channel_open(self, channel_id: ty.Optional[int] = None) -> ChannelOpenResult:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_connection_closed()

            assert self._tune_channel_max is not None

            if channel_id is None:
                channel_id = self._channel_ids.get_free()

                if channel_id > self._tune_channel_max:
                    self._channel_ids.bad(channel_id)
                    raise NoFreeChannelIdentifiersError()

            else:
                if channel_id in self._channels:
                    raise ChannelAlreadyOpenError()

                if channel_id <= 0 or channel_id > self._tune_channel_max:
                    raise ValueError("Channel id must be > 0 and <= %d", self._tune_channel_max)

                self._channel_ids.use(channel_id)

            try:
                request = Channel.Open()
                await self._rpc(channel_id, request)
            except Exception:
                self._channel_ids.free(channel_id)
                raise

            self._channels[channel_id] = self.ChannelData()

            self._wakeup(('channel_open', channel_id))

            future = self._loop.create_future()
            self._reasons[('channel_close', channel_id)] = future

            return ChannelOpenResult(
                channel_id=channel_id,
                close_reason=future
            )

    async def channel_close(self, channel_id: int) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Channel.Close(reply_code=REPLY_CODE.SUCCESS)
            try:
                await self._rpc(channel_id, request)
            except Exception:
                self._close_channel(channel_id)
                raise

            self._close_channel(channel_id)

    async def channel_flow(self, channel_id: int, active: ty.Optional[bool] = None) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Channel.Flow(active=active)
            await self._rpc(channel_id, request)

    async def exchange_declare(
            self,
            channel_id: int,
            exchange: str,
            exchange_type: str = EXCHANGE_TYPE.DIRECT,
            passive: bool = False,
            durable: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_declare(
            channel_id,
            exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            arguments=arguments
        )

    async def exchange_declare_nowait(
            self,
            channel_id: int,
            exchange: str,
            exchange_type: str = EXCHANGE_TYPE.DIRECT,
            passive: bool = False,
            durable: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_declare(
            channel_id,
            exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            arguments=arguments,
            nowait=True
        )

    async def _exchange_declare(
            self,
            channel_id: int,
            exchange: str,
            exchange_type: str = EXCHANGE_TYPE.DIRECT,
            passive: bool = False,
            durable: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> None:
        if exchange_type not in EXCHANGE_TYPE:
            raise ValueError("Invalid exchange type")

        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Exchange.Declare(
                exchange=exchange,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                nowait=nowait,
                arguments=arguments or {}
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def exchange_delete(
            self,
            channel_id: int,
            exchange: str,
            if_unused: bool = False
    ) -> None:
        await self._exchange_delete(
            channel_id,
            exchange,
            if_unused=if_unused
        )

    async def exchange_delete_nowait(
            self,
            channel_id: int,
            exchange: str,
            if_unused: bool = False
    ) -> None:
        await self._exchange_delete(
            channel_id,
            exchange,
            if_unused=if_unused,
            nowait=True
        )

    async def _exchange_delete(
            self,
            channel_id: int,
            exchange: str,
            if_unused: bool = False,
            nowait: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Exchange.Delete(
                exchange=exchange,
                if_unused=if_unused,
                nowait=nowait
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def exchange_bind(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_bind(
            channel_id,
            exchange_destination,
            exchange_source,
            routing_key,
            arguments=arguments
        )

    async def exchange_bind_nowait(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_bind(
            channel_id,
            exchange_destination,
            exchange_source,
            routing_key,
            arguments=arguments,
            nowait=True
        )

    async def _exchange_bind(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Exchange.Bind(
                destination=exchange_destination,
                source=exchange_source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments or {}
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def exchange_unbind(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_unbind(
            channel_id,
            exchange_destination,
            exchange_source,
            routing_key,
            arguments=arguments
        )

    async def exchange_unbind_nowait(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._exchange_unbind(
            channel_id,
            exchange_destination,
            exchange_source,
            routing_key,
            arguments=arguments,
            nowait=True
        )

    async def _exchange_unbind(
            self,
            channel_id: int,
            exchange_destination: str,
            exchange_source: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Exchange.Unbind(
                destination=exchange_destination,
                source=exchange_source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments or {},
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def queue_declare(
            self,
            channel_id: int,
            queue: str = '',
            passive: bool = False,
            durable: bool = False,
            exclusive: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> QueueDeclareResult:
        result = await self._queue_declare(
            channel_id,
            queue=queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments
        )
        assert result is not None
        return result

    async def queue_declare_nowait(
            self,
            channel_id: int,
            queue: str = '',
            passive: bool = False,
            durable: bool = False,
            exclusive: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._queue_declare(
            channel_id,
            queue=queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            nowait=True
        )

    async def _queue_declare(
            self,
            channel_id: int,
            queue: str = '',
            passive: bool = False,
            durable: bool = False,
            exclusive: bool = False,
            auto_delete: bool = False,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> ty.Optional[QueueDeclareResult]:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Queue.Declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                nowait=nowait,
                arguments=arguments or {}
            )
            response = await self._rpc(channel_id, request, nowait=nowait)

            if response is None:
                return None

            assert isinstance(response, Queue.DeclareOk)

            return QueueDeclareResult(
                queue=response.queue,
                message_count=response.message_count,
                consumer_count=response.consumer_count
            )

    async def queue_delete(
            self,
            channel_id: int,
            queue: str,
            if_unused: bool = False,
            if_empty: bool = False
    ) -> QueueDeleteResult:
        result = await self._queue_delete(
            channel_id,
            queue,
            if_unused=if_unused,
            if_empty=if_empty
        )
        assert result is not None
        return result

    async def queue_delete_nowait(
            self,
            channel_id: int,
            queue: str,
            if_unused: bool = False,
            if_empty: bool = False
    ) -> None:
        await self._queue_delete(
            channel_id,
            queue,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=True
        )

    async def _queue_delete(
            self,
            channel_id: int,
            queue: str,
            if_unused: bool = False,
            if_empty: bool = False,
            nowait: bool = False
    ) -> ty.Optional[QueueDeleteResult]:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Queue.Delete(
                queue=queue,
                if_unused=if_unused,
                if_empty=if_empty,
                nowait=nowait
            )
            response = await self._rpc(channel_id, request, nowait=nowait)

            if response is None:
                return None

            assert isinstance(response, Queue.DeleteOk)

            return QueueDeleteResult(
                message_count=response.message_count
            )

    async def queue_bind(
            self,
            channel_id: int,
            queue: str,
            exchange: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._queue_bind(
            channel_id,
            queue,
            exchange,
            routing_key,
            arguments=arguments
        )

    async def queue_bind_nowait(
            self,
            channel_id: int,
            queue: str,
            exchange: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        await self._queue_bind(
            channel_id,
            queue,
            exchange,
            routing_key,
            arguments=arguments,
            nowait=True
        )

    async def _queue_bind(
            self,
            channel_id: int,
            queue: str,
            exchange: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Queue.Bind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments or {}
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def queue_unbind(
            self,
            channel_id: int,
            queue: str,
            exchange: str,
            routing_key: str,
            arguments: ty.Optional[T_Arguments] = None
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Queue.Unbind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {}
            )
            await self._rpc(channel_id, request)

    async def queue_purge(
            self,
            channel_id: int,
            queue: str
    ) -> QueuePurgeResult:
        result = await self._queue_purge(
            channel_id,
            queue
        )
        assert result is not None
        return result

    async def queue_purge_nowait(
            self,
            channel_id: int,
            queue: str
    ) -> None:
        await self._queue_purge(
            channel_id,
            queue,
            nowait=True
        )

    async def _queue_purge(
            self,
            channel_id: int,
            queue: str,
            nowait: bool = False
    ) -> ty.Optional[QueuePurgeResult]:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)
            request = Queue.Purge(
                queue=queue,
                nowait=nowait
            )
            response = await self._rpc(channel_id, request, nowait=nowait)

            if response is None:
                return None

            assert isinstance(response, Queue.PurgeOk)

            return QueuePurgeResult(
                message_count=response.message_count
            )

    async def basic_publish(
            self,
            channel_id: int,
            payload: bytes,
            exchange: str,
            routing_key: str,
            properties: ty.Optional[T_Properties] = None,
            mandatory: bool = False,
            immediate: bool = False
    ) -> None:
        await self._basic_publish(
            channel_id,
            payload,
            exchange,
            routing_key,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate
        )

    async def basic_publish_nowait(
            self,
            channel_id: int,
            payload: bytes,
            exchange: str,
            routing_key: str,
            properties: ty.Optional[T_Properties] = None,
            mandatory: bool = False,
            immediate: bool = False
    ) -> None:
        await self._basic_publish(
            channel_id,
            payload,
            exchange,
            routing_key,
            properties=properties,
            mandatory=mandatory,
            immediate=immediate,
            nowait=True
        )

    async def _basic_publish(
            self,
            channel_id: int,
            payload: bytes,
            exchange: str,
            routing_key: str,
            properties: ty.Optional[T_Properties] = None,
            mandatory: bool = False,
            immediate: bool = False,
            nowait: bool = False
    ) -> None:
        future = None
        try:
            async with self._channel_lock.lock(channel_id):
                self._raise_if_channel_closed(channel_id)
                assert self._tune_frame_max is not None

                channel = self._channels[channel_id]

                if channel.publisher_confirm:
                    delivery_tag = next(channel.delivery_tags)
                    channel.unconfirmed_delivery_tags.add(delivery_tag)

                    if not nowait:
                        future = self._create_waiter(('confirm', channel_id, delivery_tag))

                basic_publish_frame = Basic.Publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    mandatory=mandatory,
                    immediate=immediate
                )

                await self._send_frame(channel_id, basic_publish_frame, drain=False)

                if properties is None:
                    properties = {'delivery_mode': DELIVERY_MODE.TRANSIENT}

                content_header_frame = ContentHeader(
                    body_size=len(payload),
                    properties=Basic.Properties(**properties)
                )
                await self._send_frame(channel_id, content_header_frame, drain=False)

                for chunk in chunk_iter(payload, self._tune_frame_max - FRAME_HEADER_SIZE - FRAME_END_SIZE):
                    content_body_frame = ContentBody(chunk)
                    await self._send_frame(channel_id, content_body_frame, drain=False)

                await self._drain()

            if future is not None:
                await future
        finally:
            if future is not None and not future.done():
                future.cancel()

    async def basic_qos(
            self,
            channel_id: int,
            prefetch_size: int = 0,
            prefetch_count: int = 0,
            connection_global: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.Qos(
                prefetch_size=prefetch_size,
                prefetch_count=prefetch_count,
                global_=connection_global
            )
            await self._rpc(channel_id, request)

    async def basic_consume(
            self,
            channel_id: int,
            queue: str = '',
            consumer_tag: ty.Optional[str] = None,
            no_local: bool = False,
            no_ack: bool = False,
            exclusive: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> BasicConsumeResult:
        return await self._basic_consume(
            channel_id,
            queue=queue,
            consumer_tag=consumer_tag,
            no_local=no_local,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments
        )

    async def basic_consume_nowait(
            self,
            channel_id: int,
            queue: str = '',
            consumer_tag: ty.Optional[str] = None,
            no_local: bool = False,
            no_ack: bool = False,
            exclusive: bool = False,
            arguments: ty.Optional[T_Arguments] = None
    ) -> BasicConsumeResult:
        return await self._basic_consume(
            channel_id,
            queue=queue,
            consumer_tag=consumer_tag,
            no_local=no_local,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
            nowait=True
        )

    async def _basic_consume(
            self,
            channel_id: int,
            queue: str = '',
            consumer_tag: ty.Optional[str] = None,
            no_local: bool = False,
            no_ack: bool = False,
            exclusive: bool = False,
            arguments: ty.Optional[T_Arguments] = None,
            nowait: bool = False
    ) -> BasicConsumeResult:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            channel = self._channels[channel_id]

            if consumer_tag is None:
                while True:
                    consumer_tag = 'ctag%i.%s' % (channel_id, uuid.uuid4().hex)
                    if consumer_tag not in channel.consumer_tags:
                        break

            elif consumer_tag in channel.consumer_tags:
                raise AlreadyConsumedError()

            request = Basic.Consume(
                queue=queue,
                consumer_tag=consumer_tag,
                no_local=no_local,
                no_ack=no_ack,
                exclusive=exclusive,
                nowait=nowait,
                arguments=arguments or {}
            )

            response = await self._rpc(channel_id, request, nowait=nowait)

            channel.consumer_tags.add(consumer_tag)

            future = self._loop.create_future()
            self._reasons[('consumer_cancel', channel_id, consumer_tag)] = future

            self._wakeup(('consumer_start', consumer_tag))

            if response is None:
                return BasicConsumeResult(
                    consumer_tag=consumer_tag,
                    cancel_reason=future
                )

            else:
                assert isinstance(response, Basic.ConsumeOk)
                return BasicConsumeResult(
                    consumer_tag=response.consumer_tag,
                    cancel_reason=future
                )

    async def basic_cancel(
            self,
            channel_id: int,
            consumer_tag: str
    ) -> ty.Optional[BasicCancelResult]:
        result = await self._basic_cancel(
            channel_id,
            consumer_tag
        )
        assert result is not None
        return result

    async def basic_cancel_nowait(
            self,
            channel_id: int,
            consumer_tag: str
    ) -> None:
        await self._basic_cancel(
            channel_id,
            consumer_tag,
            nowait=True
        )

    async def _basic_cancel(
            self,
            channel_id: int,
            consumer_tag: str,
            nowait=False
    ) -> ty.Optional[BasicCancelResult]:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_consume_cancelled(channel_id, consumer_tag)

            self._cancel_consume(channel_id, consumer_tag)

            request = Basic.Cancel(
                consumer_tag=consumer_tag,
                nowait=nowait
            )
            response = await self._rpc(channel_id, request, nowait=nowait)

            if response is None:
                return None

            assert isinstance(response, Basic.CancelOk)

            return BasicCancelResult(
                consumer_tag=response.consumer_tag
            )

    async def basic_get(
            self,
            channel_id: int,
            queue='',
            no_ack=False
    ) -> ReceivedMessage:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            future = self._create_waiter(('get', channel_id))
            try:

                request = Basic.Get(
                    queue=queue,
                    no_ack=no_ack
                )
                response = await self._rpc(channel_id, request)

                if isinstance(response, Basic.GetEmpty):
                    raise QueueEmptyError()

                message = await future

                assert isinstance(message, ReceivedMessage)

                return message
            finally:
                if not future.done():
                    future.cancel()

    async def basic_ack(
            self,
            channel_id: int,
            delivery_tag: int,
            multiple: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.Ack(
                delivery_tag=delivery_tag,
                multiple=multiple
            )
            await self._rpc(channel_id, request, nowait=True)

    async def basic_nack(
            self,
            channel_id: int,
            delivery_tag: int,
            multiple: bool = False,
            requeue: bool = True
    ) -> None:
        if not self.server_implemented_basic_nack:
            raise MethodNotImplementedError()

        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.Nack(
                delivery_tag=delivery_tag,
                multiple=multiple,
                requeue=requeue
            )
            await self._rpc(channel_id, request, nowait=True)

    async def basic_reject(
            self,
            channel_id: int,
            delivery_tag: int,
            requeue: bool = False
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.Reject(
                delivery_tag=delivery_tag,
                requeue=requeue
            )
            await self._rpc(channel_id, request, nowait=True)

    async def basic_recover_async(
            self,
            channel_id: int,
            requeue: bool = True
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.RecoverAsync(
                requeue=requeue
            )
            await self._rpc(channel_id, request, nowait=True)

    async def basic_recover(
            self,
            channel_id: int,
            requeue: bool = True
    ) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            request = Basic.Recover(
                requeue=requeue
            )
            await self._rpc(channel_id, request)

    async def confirm_select(
            self,
            channel_id: int
    ) -> None:
        await self._confirm_select(
            channel_id
        )

    async def confirm_select_nowait(
            self,
            channel_id: int
    ) -> None:
        await self._confirm_select(
            channel_id,
            nowait=True
        )

    async def _confirm_select(
            self,
            channel_id: int,
            nowait: bool = False
    ) -> None:
        if not self.publisher_confirms:
            raise MethodNotImplementedError()

        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)

            channel = self._channels[channel_id]
            channel.publisher_confirm = True

            request = Confirm.Select(
                nowait=nowait
            )
            await self._rpc(channel_id, request, nowait=nowait)

    async def tx_commit(self, channel_id: int) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)
            await self._rpc(channel_id, Tx.Commit())

    async def tx_rollback(self, channel_id: int) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)
            await self._rpc(channel_id, Tx.Rollback())

    async def tx_select(self, channel_id: int) -> None:
        async with self._channel_lock.lock(channel_id):
            self._raise_if_channel_closed(channel_id)
            await self._rpc(channel_id, Tx.Select())

    async def wait_for_connection_open(self) -> None:
        if self._connection_open:
            return

        await self._wait('connection_open', cancel_previous=False)

    async def wait_for_connection_close(self) -> None:
        if not self._connection_open:
            return

        await self._wait('connection_close', cancel_previous=False)

    async def wait_for_channel_open(self, channel_id: int) -> None:
        if channel_id in self._channels:
            return

        await self._wait(('channel_open', channel_id), cancel_previous=False)

    async def wait_for_channel_close(self, channel_id: int) -> None:
        if channel_id not in self._channels:
            return

        await self._wait(('channel_close', channel_id), cancel_previous=False)

    async def wait_for_consumer_start(self, channel_id: int, consumer_tag: str) -> None:
        channel = self._channels.get(channel_id)

        if channel is not None:
            if consumer_tag in channel.consumer_tags:
                return

        await self._wait(('consumer_start', consumer_tag), cancel_previous=False)

    async def wait_for_consumer_cancel(self, channel_id: int, consumer_tag: str) -> None:
        channel = self._channels.get(channel_id)

        if channel is None:
            return

        if consumer_tag not in channel.consumer_tags:
            return

        await self._wait(('consumer_cancel', consumer_tag), cancel_previous=False)

    @ty.overload
    def delivered_messages(self) -> ty.AsyncGenerator[DeliveredMessage, None]:
        ...

    @ty.overload
    def delivered_messages(self, channel_id: int) -> ty.AsyncGenerator[DeliveredMessage, None]:
        ...

    @ty.overload
    def delivered_messages(self, channel_id: int, consumer_tag: str) -> ty.AsyncGenerator[DeliveredMessage, None]:
        ...

    def delivered_messages(
            self,
            channel_id: ty.Optional[int] = None,
            consumer_tag: ty.Optional[str] = None
    ) -> ty.AsyncGenerator[DeliveredMessage, None]:
        return self._consumer(('delivered_messages', channel_id, consumer_tag))

    def returned_messages(self, channel_id: ty.Optional[int] = None) -> ty.AsyncGenerator[ReturnedMessage, None]:
        return self._consumer(('returned_messages', channel_id))

    def is_connection_open(self) -> bool:
        return self._connection_open

    def is_channel_open(self, channel_id: int) -> bool:
        if not self._connection_open:
            return False

        return channel_id in self._channels

    def has_consumer(self, channel_id: int, consumer_tag: str) -> bool:
        if not self._connection_open:
            return False

        if channel_id not in self._channels:
            return False

        return consumer_tag in self._channels[channel_id].consumer_tags

    @property
    def server_capabilities(self) -> T_Arguments:
        self._raise_if_connection_closed()
        assert self._server_properties is not None
        return self._server_properties['capabilities']

    @property
    def server_implemented_basic_nack(self) -> bool:
        return bool(self.server_capabilities.get('basic.nack'))

    @property
    def consumer_cancel_notify(self) -> bool:
        return bool(self.server_capabilities.get('consumer_cancel_notify'))

    @property
    def exchange_exchange_bindings(self) -> bool:
        return bool(self.server_capabilities.get('exchange_exchange_bindings'))

    @property
    def publisher_confirms(self) -> bool:
        return bool(self.server_capabilities.get('publisher_confirms'))

    async def _consumer(self, consumer_key: ty.Hashable) -> ty.AsyncGenerator[ty.Any, None]:
        queue: aio.Queue = aio.Queue(loop=self._loop)
        self._consumer_queues[consumer_key].add(queue)
        try:
            while True:
                message = await queue.get()
                try:
                    yield message
                finally:
                    queue.task_done()
        finally:
            self._consumer_queues[consumer_key].discard(queue)
            if not self._consumer_queues[consumer_key]:
                self._consumer_queues.pop(consumer_key, None)

    @staticmethod
    def _raise_if_connection_close_received(frame: ty.Any) -> None:
        if isinstance(frame, Connection.Close):
            raise ConnectionClosedByServerError(
                reply_code=frame.reply_code,
                reply_text=frame.reply_text,
                class_id=frame.class_id,
                method_id=frame.method_id
            )

    def _maybe_put_message(self, consumer_keys: ty.Iterable[ty.Hashable], message: ty.Any) -> None:
        for consumer_key in consumer_keys:
            queues = self._consumer_queues.get(consumer_key)
            if queues is not None:
                for queue in queues:
                    queue.put_nowait(message)

    def _maybe_put_delivered_message(self, message: DeliveredMessage) -> None:
        consumer_keys = (
            ('delivered_messages', None, None),
            ('delivered_messages', message.channel_id, None),
            ('delivered_messages', message.channel_id, message.consumer_tag)
        )
        self._maybe_put_message(consumer_keys, message)

    def _maybe_put_returned_message(self, message: ReturnedMessage) -> None:
        consumer_keys = (
            ('returned_messages', None),
            ('returned_messages', message.channel_id)
        )
        self._maybe_put_message(consumer_keys, message)

    def _task_done_callback(self, future: aio.Future) -> None:
        exception = None
        try:
            exception = future.exception()
        except aio.CancelledError:
            pass

        if exception is not None:
            self._close_connection(exception=exception)
        
    def _raise_if_connection_closed(self) -> None:
        if not self._connection_open:
            raise ConnectionClosedError()

    def _raise_if_channel_closed(self, channel_id: int) -> None:
        self._raise_if_connection_closed()

        if channel_id not in self._channels:
            raise ChannelClosedError()

    def _raise_if_consume_cancelled(self, channel_id: int, consumer_tag: str) -> None:
        self._raise_if_channel_closed(channel_id)

        if consumer_tag not in self._channels[channel_id].consumer_tags:
            raise ConsumerCancelledError()

    async def _read_content(self) -> ty.Tuple[bytes, T_Properties]:
        _, _, content_header_frame = await self._receive_frame()
        assert isinstance(content_header_frame, ContentHeader)

        chunks: ty.List[bytes] = []
        num_bytes_read = 0
        while num_bytes_read < content_header_frame.body_size:
            _, _, content_body_frame = await self._receive_frame()
            assert isinstance(content_body_frame, ContentBody)
            chunk = content_body_frame.value
            num_bytes_read += len(chunk)
            chunks.append(chunk)

        return b''.join(chunks), content_header_frame.properties.to_dict()

    def _close_connection(self, exception: ty.Optional[BaseException] = None) -> None:
        if not self._connection_open:
            return

        self._connection_open = False

        for channel_id in list(self._channels):
            self._close_channel(channel_id, exception=exception)

        self._channel_ids.reset()

        if self._writer is not None:
            try:
                self._writer.close()
            except:  # noqa: E722
                pass
            self._writer = None

        if self._reader is not None:
            self._reader = None

        for task in self._tasks:
            if not task.done():
                task.cancel()

        self._tasks.clear()

        self._server_properties = None
        self._tune_channel_max = None
        self._tune_frame_max = None
        self._tune_heartbeat = None

        self._last_receive_time = None

        future = self._reasons.pop('connection_close', None)
        if future is not None and not future.done():
            if exception is not None:
                future.set_exception(exception)

            else:
                future.set_result(None)

        self._wakeup('connection_close')

        if exception is None:
            exception = ConnectionCloseForcedError()

        self._wakeup_all(exception=exception)

    async def _handle_close(self, frame: Connection.Close) -> None:
        connection_close_ok_frame = Connection.CloseOk()
        await self._send_frame(0, connection_close_ok_frame)

        self._close_connection(exception=ConnectionClosedByServerError(
            reply_code=frame.reply_code,
            reply_text=frame.reply_text,
            class_id=frame.class_id,
            method_id=frame.method_id
        ))

    def _close_channel(self, channel_id: int, exception: ty.Optional[BaseException] = None) -> None:
        channel = self._channels.get(channel_id)

        if channel is None:
            return

        self._channel_ids.free(channel_id)

        for consumer_tag in list(channel.consumer_tags):
            self._cancel_consume(channel_id, consumer_tag, exception=exception)

        future = self._reasons.pop(('channel_close', channel_id), None)
        if future is not None and not future.done():
            if exception is not None:
                future.set_exception(exception)

            else:
                future.set_result(None)

        self._wakeup(('channel_close', channel_id))

        if exception is None:
            exception = ChannelCloseForcedError()

        self._wakeup(('response', channel_id), exception=exception)
        self._wakeup(('get', channel_id), exception=exception)

        del self._channels[channel_id]

    async def _handle_channel_close(self, channel_id: int, frame: Channel.Close) -> None:
        self._close_channel(
            channel_id,
            exception=ChannelClosedByServerError(
                channel_id=channel_id,
                reply_code=frame.reply_code,
                reply_text=frame.reply_text,
                class_id=frame.class_id,
                method_id=frame.method_id
            )
        )

        channel_close_ok_frame = Channel.CloseOk()
        await self._send_frame(channel_id, channel_close_ok_frame)

    async def _handle_basic_get_ok(self, channel_id: int, frame: Basic.GetOk) -> None:
        body, properties = await self._read_content()

        message = ReceivedMessage(
            body=body,
            channel_id=channel_id,
            delivery_tag=frame.delivery_tag,
            exchange=frame.exchange,
            properties=properties,
            redelivered=frame.redelivered,
            routing_key=frame.routing_key,
            timestamp=datetime.datetime.utcnow()
        )

        self._wakeup(('get', channel_id), result=message)

    def _cancel_consume(self, channel_id: int, consumer_tag: str, exception: ty.Optional[BaseException] = None) -> None:
        channel = self._channels.get(channel_id)

        if channel is None:
            return

        if consumer_tag not in channel.consumer_tags:
            return

        channel.consumer_tags.remove(consumer_tag)

        future = self._reasons.pop(('consumer_cancel', channel_id, consumer_tag), None)
        if future is not None and not future.done():
            if exception is not None:
                future.set_exception(exception)

            else:
                future.set_result(None)

        self._wakeup(('consumer_cancel', consumer_tag))

    async def _handle_basic_cancel(self, channel_id: int, frame: Basic.Cancel) -> None:
        consumer_tag = frame.consumer_tag
        self._cancel_consume(
            channel_id,
            consumer_tag,
            exception=ConsumerCancelledByServerError(
                channel_id=channel_id,
                consumer_tag=consumer_tag
            )
        )

        if not frame.nowait:
            basic_cancel_ok_frame = Basic.CancelOk(consumer_tag=consumer_tag)
            await self._send_frame(channel_id, basic_cancel_ok_frame)

    async def _handle_basic_deliver(
            self,
            channel_id: int,
            frame: Basic.Deliver
    ) -> None:
        body, properties = await self._read_content()

        message = DeliveredMessage(
            body=body,
            channel_id=channel_id,
            consumer_tag=frame.consumer_tag,
            delivery_tag=frame.delivery_tag,
            exchange=frame.exchange,
            properties=properties,
            redelivered=frame.redelivered,
            routing_key=frame.routing_key,
            timestamp=datetime.datetime.utcnow()
        )

        self._maybe_put_delivered_message(message)

    async def _handle_basic_return(self, channel_id: int, frame: Basic.Return) -> None:
        body, properties = await self._read_content()

        message = ReturnedMessage(
            body=body,
            channel_id=channel_id,
            exchange=frame.exchange,
            properties=properties,
            reply_code=frame.reply_code,
            reply_text=frame.reply_text,
            routing_key=frame.routing_key,
            timestamp=datetime.datetime.utcnow()
        )

        self._maybe_put_returned_message(message)

    def _handle_confirm(self, channel_id: int, frame: ty.Union[Basic.Ack, Basic.Nack]) -> None:
        exception = None
        if isinstance(frame, Basic.Nack):
            exception = PublishFailedError()

        self._wakeup(('confirm', channel_id, frame.delivery_tag), exception=exception)

        channel = self._channels.get(channel_id)
        if channel is None:
            return

        if frame.multiple:
            for delivery_tag in channel.unconfirmed_delivery_tags:
                if delivery_tag < frame.delivery_tag:
                    self._wakeup(('confirm', channel_id, delivery_tag), exception=exception)

    def _create_waiter(self, key: ty.Hashable, cancel_previous: bool = True) -> aio.Future:
        waiters = self._waiters[key]

        if cancel_previous:
            for future in waiters:
                if not future.done():
                    future.cancel()

        def done_callback(future: aio.Future) -> None:
            waiters.discard(future)
            if not waiters:
                del self._waiters[key]

        future = self._loop.create_future()
        future.add_done_callback(done_callback)

        waiters.add(future)

        return future

    async def _wait(self, key: ty.Hashable, cancel_previous: bool = True) -> ty.Any:
        return await self._create_waiter(key, cancel_previous=cancel_previous)

    def _wakeup(
            self,
            key: ty.Hashable,
            result: ty.Optional[ty.Any] = None,
            exception: ty.Optional[BaseException] = None
    ) -> None:
        waiters = self._waiters.get(key)

        if waiters is None:
            return

        for future in waiters:
            if future.done():
                continue

            if result is not None:
                future.set_result(result)

            elif exception is not None:
                future.set_exception(exception)

            else:
                future.set_result(None)

    def _wakeup_all(
            self,
            result: ty.Optional[ty.Any] = None,
            exception: ty.Optional[BaseException] = None
    ) -> None:
        for key in self._waiters.keys():
            self._wakeup(key, result=result, exception=exception)

    async def _rpc(
            self,
            channel_id: int,
            request: Frame,
            nowait: bool = False
    ) -> ty.Optional[Frame]:
        if nowait:
            future = None

        else:
            future = self._create_waiter(('response', channel_id))

        try:
            await self._send_frame(channel_id, request)

            if future is None:
                return None

            response = await future

            assert isinstance(response, Frame)

            if response.name in response.valid_responses:
                raise UnexpectedFrameReceivedError()

            return response
        finally:
            if future is not None and not future.done():
                future.cancel()

    async def _drain(self) -> None:
        assert self._writer is not None

        try:
            async with self._drain_lock:
                await self._writer.drain()
        except ConnectionResetError as e:
            raise ConnectionLostError() from e

    async def _send(self, data: bytes, drain: bool = True) -> None:
        assert self._writer is not None

        try:
            self._writer.write(data)
        except ConnectionResetError as e:
            raise ConnectionLostError() from e

        if drain:
            await self._drain()

    async def _send_frame(self, channel_id: int, frame: T_AnyFrame, drain: bool = True) -> None:
        await self._send(ty.cast(bytes, marshal(frame, channel_id)), drain=drain)

    async def _receive_frame(self) -> ty.Tuple[int, int, ty.Any]:
        assert self._reader is not None

        try:
            frame_header = await self._reader.readexactly(FRAME_HEADER_SIZE)
            _, _, frame_length = frame_parts(frame_header)
            frame_payload = await self._reader.readexactly(frame_length + FRAME_END_SIZE)
        except (ConnectionResetError, aio.IncompleteReadError) as e:
            raise ConnectionLostError() from e

        self._last_receive_time = time_func()

        try:
            return unmarshal(b''.join((frame_header, frame_payload)))
        except UnmarshalingException as e:
            raise UnmarshallingError() from e

    async def _receive(self) -> None:
        _, channel_id, frame = await self._receive_frame()

        if channel_id == 0:
            if isinstance(frame, Heartbeat):
                return

            elif isinstance(frame, Connection.Close):
                await self._handle_close(frame)
                return

        else:
            if isinstance(frame, Basic.GetOk):
                await self._handle_basic_get_ok(channel_id, frame)

            elif isinstance(frame, Basic.Deliver):
                await self._handle_basic_deliver(channel_id, frame)
                return

            elif isinstance(frame, Basic.Return):
                await self._handle_basic_return(channel_id, frame)
                return

            elif isinstance(frame, Channel.Close):
                await self._handle_channel_close(channel_id, frame)
                return

            elif isinstance(frame, Basic.Cancel):
                await self._handle_basic_cancel(channel_id, frame)
                return

            elif isinstance(frame, (Basic.Ack, Basic.Nack)):
                self._handle_confirm(channel_id, frame)
                return

        self._wakeup(('response', channel_id), result=frame)

    async def _receiving_task(self) -> None:
        while True:
            await self._receive()

    @staticmethod
    def _negotiate_integer_value(client_value: ty.Optional[int], server_value: ty.Optional[int]) -> int:
        if client_value is None:
            client_value = 0

        if server_value is None:
            server_value = 0

        if client_value == 0 or server_value == 0:
            return max(client_value, server_value)

        else:
            return min(client_value, server_value)

    @staticmethod
    def _tune_heartbeat_timeout(client_value: ty.Optional[int], server_value: int) -> int:
        if client_value is None:
            return server_value

        else:
            return client_value

    async def _heart_task(self) -> None:
        assert self._tune_heartbeat is not None
        heartbeat_interval = self._tune_heartbeat * self._heartbeat_interval_multiplier
        next_heartbeat_time = time_func() + heartbeat_interval
        while True:
            await aio.sleep(1, loop=self._loop)
            if next_heartbeat_time <= time_func():
                heartbeat_frame = Heartbeat()
                await self._send_frame(0, heartbeat_frame)
                next_heartbeat_time = time_func() + heartbeat_interval

    async def _heartbeat_monitor_task(self) -> None:
        assert self._tune_heartbeat is not None
        heartbeat_grace_timeout = self._tune_heartbeat * self._heartbeat_grace_multiplier
        while True:
            await aio.sleep(1, loop=self._loop)
            if (
                    self._last_receive_time is not None and
                    time_func() - self._last_receive_time > heartbeat_grace_timeout
            ):
                raise ServerDiedError()
