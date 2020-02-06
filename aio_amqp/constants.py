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

import platform
import typing as ty

__all__ = (
    'CLASS_ID',
    'DELIVERY_MODE',
    'EXCHANGE_TYPE',
    'METHOD_ID',
    'REPLY_CODE',
    'FRAME_END_SIZE',
    'FRAME_HEADER_SIZE',
    'PROTOCOL_VERSION',
    'PLATFORM',
)


class ClassId(ty.NamedTuple):
    CONNECTION: int = 10
    CHANNEL: int = 20
    EXCHANGE: int = 40
    QUEUE: int = 50
    BASIC: int = 60
    TX: int = 90
    CONFIRM: int = 85


CLASS_ID = ClassId()


class DeliveryMode(ty.NamedTuple):
    TRANSIENT: int = 1
    PERSISTENT: int = 2


DELIVERY_MODE = DeliveryMode()


class ExchangeType(ty.NamedTuple):
    DIRECT: str = 'direct'
    FANOUT: str = 'fanout'
    TOPIC: str = 'topic'


EXCHANGE_TYPE = ExchangeType()


class MethodId(ty.NamedTuple):
    CONNECTION_START: int = 10
    CONNECTION_START_OK: int = 11
    CONNECTION_SECURE: int = 20
    CONNECTION_SECURE_OK: int = 21
    CONNECTION_TUNE: int = 30
    CONNECTION_TUNE_OK: int = 31
    CONNECTION_OPEN: int = 40
    CONNECTION_OPEN_OK: int = 41
    CONNECTION_CLOSE: int = 50
    CONNECTION_CLOSE_OK: int = 51

    CHANNEL_OPEN: int = 10
    CHANNEL_OPEN_OK: int = 11
    CHANNEL_FLOW: int = 20
    CHANNEL_FLOW_OK: int = 21
    CHANNEL_CLOSE: int = 40
    CHANNEL_CLOSE_OK: int = 41

    EXCHANGE_DECLARE: int = 10
    EXCHANGE_DECLARE_OK: int = 11
    EXCHANGE_DELETE: int = 20
    EXCHANGE_DELETE_OK: int = 21
    EXCHANGE_BIND: int = 30
    EXCHANGE_BIND_OK: int = 31
    EXCHANGE_UNBIND: int = 40
    EXCHANGE_UNBIND_OK: int = 51

    QUEUE_DECLARE: int = 10
    QUEUE_DECLARE_OK: int = 11
    QUEUE_BIND: int = 20
    QUEUE_BIND_OK: int = 21
    QUEUE_UNBIND: int = 50
    QUEUE_UNBIND_OK: int = 51
    QUEUE_PURGE: int = 30
    QUEUE_PURGE_OK: int = 31
    QUEUE_DELETE: int = 40
    QUEUE_DELETE_OK: int = 41

    BASIC_QOS: int = 10
    BASIC_QOS_OK: int = 11
    BASIC_CONSUME: int = 20
    BASIC_CONSUME_OK: int = 21
    BASIC_CANCEL: int = 30
    BASIC_CANCEL_OK: int = 31
    BASIC_PUBLISH: int = 40
    BASIC_RETURN: int = 50
    BASIC_DELIVER: int = 60
    BASIC_GET: int = 70
    BASIC_GET_OK: int = 71
    BASIC_GET_EMPTY: int = 72
    BASIC_ACK: int = 80
    BASIC_REJECT: int = 90
    BASIC_RECOVER_ASYNC: int = 100
    BASIC_RECOVER: int = 110
    BASIC_RECOVER_OK: int = 111
    BASIC_NACK: int = 120

    TX_SELECT: int = 10
    TX_SELECT_OK: int = 11
    TX_COMMIT: int = 20
    TX_COMMIT_OK: int = 21
    TX_ROLLBACK: int = 30
    TX_ROLLBACK_OK: int = 31

    CONFIRM_SELECT: int = 10
    CONFIRM_SELECT_OK: int = 11


METHOD_ID = MethodId()


class ReplyCode(ty.NamedTuple):
    SUCCESS: int = 200
    CONTENT_TOO_LARGE: int = 311
    NO_ROUTE: int = 312
    NO_CONSUMERS: int = 313
    ACCESS_REFUSED: int = 403
    NOT_FOUND: int = 404
    RESOURCE_LOCKED: int = 405
    PRECONDITION_FAILED: int = 406
    CONNECTION_FORCED: int = 320
    INVALID_PATH: int = 402
    FRAME_ERROR: int = 501
    SYNTAX_ERROR: int = 502
    COMMAND_INVALID: int = 503
    CHANNEL_ERROR: int = 504
    UNEXPECTED_FRAME: int = 505
    RESOURCE_ERROR: int = 506
    NOT_ALLOWED: int = 530
    NOT_IMPLEMENTED: int = 540
    INTERNAL_ERROR: int = 541


REPLY_CODE = ReplyCode()

FRAME_END_SIZE = 1
FRAME_HEADER_SIZE = 7

PROTOCOL_VERSION = (0, 9, 1)

PLATFORM = '%s %s' % (
    platform.python_implementation(),
    platform.python_version(),
)
