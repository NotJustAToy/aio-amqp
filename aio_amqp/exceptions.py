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

__all__ = (
    'Error',
    'ProtocolError',
    'UnexpectedFrameReceivedError',
    'UnsupportedAuthMechanismsError',
    'UnmarshallingError',
    'ProtocolIncompatibleError',
    'MethodNotImplementedError',
    'ConnectionAlreadyOpenError',
    'ConnectFailedError',
    'ConnectionClosedError',
    'ServerDiedError',
    'ConnectionLostError',
    'ConnectionCloseForcedError',
    'ConnectionClosedByServerError',
    'ChannelAlreadyOpenError',
    'ChannelClosedError',
    'ChannelCloseForcedError',
    'ChannelClosedByServerError',
    'QueueEmptyError',
    'AlreadyConsumedError',
    'ConsumerCancelledError',
    'ConsumerCancelledByServerError',
    'NoFreeChannelIdentifiersError',
    'PublishFailedError',
)


class Error(Exception):
    pass


class ProtocolError(Error):
    pass


class UnexpectedFrameReceivedError(ProtocolError):
    pass


class UnsupportedAuthMechanismsError(ProtocolError):
    pass


class UnmarshallingError(ProtocolError):
    pass


class ProtocolIncompatibleError(ProtocolError):
    pass


class MethodNotImplementedError(ProtocolError):
    pass


class ConnectionAlreadyOpenError(Error):
    pass


class ConnectFailedError(Error):
    pass


class ConnectionClosedError(Error):
    pass


class ServerDiedError(ConnectionClosedError):
    pass


class ConnectionCloseForcedError(ConnectionClosedError):
    pass


class ConnectionClosedByServerError(ConnectionClosedError):

    def __init__(
            self,
            *,
            reply_code: int,
            reply_text: str,
            class_id: int,
            method_id: int,
    ) -> None:
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.class_id = class_id
        self.method_id = method_id


class ConnectionLostError(ConnectionClosedError):
    pass


class ChannelAlreadyOpenError(Error):
    pass


class ChannelClosedError(Error):
    pass


class ChannelCloseForcedError(ChannelClosedError):
    pass


class ChannelClosedByServerError(ChannelClosedError):

    def __init__(
            self,
            *,
            channel_id: int,
            reply_code: int,
            reply_text: str,
            class_id: int,
            method_id: int,
    ) -> None:
        self.channel_id = channel_id
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.class_id = class_id
        self.method_id = method_id


class QueueEmptyError(Error):
    pass


class AlreadyConsumedError(Error):
    pass


class ConsumerCancelledError(Error):
    pass


class ConsumerCancelledByServerError(ConsumerCancelledError):

    def __init__(self, *, channel_id: int, consumer_tag: str) -> None:
        self.channel_id = channel_id
        self.consumer_tag = consumer_tag


class NoFreeChannelIdentifiersError(Error):
    pass


class PublishFailedError(Error):
    pass
