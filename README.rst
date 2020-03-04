***********
AMQP client
***********

About
#####

Asynchronous AMQP client for 0.9.1 protocol version

Installation
############

Recommended way (via pip):

.. code:: bash

    $ pip install aio-amqp

Example
#######

Simple echo RPC server:

.. code:: python

    import asyncio as aio
    import logging
    import typing as ty

    import aio_amqp

    logger = logging.getLogger(__name__)


    class EchoRpcServer:

        _channel_id = 1

        def __init__(
                self,
                rpc_queue_name: str,
                heartbeat: ty.Optional[int] = None,
                reconnection_interval: int = 10,
                loop: ty.Optional[aio.AbstractEventLoop] = None
        ) -> None:
            self._rpc_queue_name = rpc_queue_name
            self._reconnection_interval = reconnection_interval
            self._loop = loop or aio.get_event_loop()
            self._client = aio_amqp.Client(heartbeat=heartbeat, loop=self._loop)
            self._tasks = [
                self._loop.create_task(self._connect_forever()),
                self._loop.create_task(self._handle_messages())
            ]

        async def close(self) -> None:
            for task in self._tasks:
                if task.done():
                    continue
                task.cancel()
                try:
                    await task
                except aio.CancelledError:
                    pass

        async def _handle_messages(self) -> None:
            async for message in self._client.delivered_messages():
                correlation_id = message.properties.get('correlation_id')
                reply_to = message.properties.get('reply_to')
                if reply_to is None:
                    logger.warning("Invalid message properties")
                    continue
                try:
                    await self._client.basic_ack(self._channel_id, message.delivery_tag)
                    properties = {}
                    if correlation_id is not None:
                        properties = {'correlation_id': correlation_id}
                    await self._client.basic_publish(
                        self._channel_id, message.body, '', reply_to, properties=properties)

                except Exception as e:
                    logger.error("Unhandled exception during echo message publishing", exc_info=e)

        async def _connect_forever(self) -> None:
            try:
                while True:
                    try:
                        logger.info("Connecting to AMQP server")
                        await self._client.connection_open('localhost')

                        channel_open_result = await self._client.channel_open(self._channel_id)

                        await self._client.queue_declare(
                            self._channel_id,
                            queue=self._rpc_queue_name,
                            auto_delete=True
                        )

                        await self._client.basic_consume(
                            self._channel_id,
                            queue=self._rpc_queue_name
                        )

                        logger.info("Monitoring for network interruptions...")
                        await channel_open_result.close_reason
                    except aio.CancelledError:
                        raise

                    except aio_amqp.ConnectFailedError as e:
                        logger.error("Connect to AMQP server failed", exc_info=e)

                    except aio_amqp.ConnectionLostError as e:
                        logger.error("Connection lost", exc_info=e)

                    except aio_amqp.ConnectionClosedByServerError as e:
                        if e.reply_code == aio_amqp.REPLY_CODE.ACCESS_REFUSED:
                            logger.error("Access refused", exc_info=e)

                        else:
                            logger.error("Connection closed by server", exc_info=e)

                    except aio_amqp.ChannelClosedByServerError as e:
                        if self._client.is_connection_open():
                            await self._client.connection_close()
                        logger.error("Channel closed by server", exc_info=e)

                    except Exception as e:
                        await self._client.connection_close()
                        logger.error("Unhandled exception during connecting", exc_info=e)
                        raise

                    else:
                        logger.info("Connection closed")
                        return

                    logger.info("Reconnect after %d seconds" % self._reconnection_interval)
                    await aio.sleep(self._reconnection_interval)
            finally:
                if self._client.is_connection_open():
                    await self._client.connection_close()


    if __name__ == '__main__':
        logging.basicConfig(
            level='DEBUG'
        )
        loop = aio.new_event_loop()
        server = EchoRpcServer('rpc_queue', reconnection_interval=10, loop=loop)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        finally:
            loop.run_until_complete(server.close())
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

License
#######

Copyright 2020 Not Just A Toy Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
