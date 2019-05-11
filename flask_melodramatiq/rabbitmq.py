import dramatiq


class RabbitmqBrokerMixin:
    def publish_message(self, message, *, exchange='', routing_key=None):
        """Publish a message on an exchange.

        :param message: The message
        :type message: dramatiq.Message

        :param exchange: The name of the RabbitMQ exchange on which to
          publish the message

        :param routing_key: The message routing key. If the value is
          `None`, the routing key will be
          ``f"dramatiq.events.{message.actor_name}"`` if
          ``message.queue_name`` is `None`, and ``message.queue_name``
          otherwise.

        """

        import pika

        if routing_key is None:
            if message.queue_name is None:
                routing_key = 'dramatiq.events.' + message.actor_name
            else:
                routing_key = message.queue_name

        properties = pika.BasicProperties(
            delivery_mode=2,
            priority=message.options.get("broker_priority"),
        )

        attempts = 1
        try:
            # In pika 1.0 the legacy `basic_publish` method is
            # removed, and `publish` renamed to `basic_publish`. So,
            # to support pika versions before and after 1.0 we need an
            # ugly hack.
            publish = self.channel.publish
        except AttributeError:
            publish = self.channel.basic_publish
        while True:
            try:
                publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message.encode(),
                    properties=properties,
                )
                return

            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPChannelError) as e:
                del self.channel
                del self.connection

                attempts += 1
                if attempts > 6:
                    raise dramatiq.ConnectionClosed(e) from None
