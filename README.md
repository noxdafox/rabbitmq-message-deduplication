RabbitMQ Message Deduplication Plugin
=====================================

A plugin for filtering duplicate messages.

Exchange Type: `x-message-deduplication`

Building from Source
--------------------

Please see RabbitMQ Plugin Development guide.

To build the plugin:

```bash
        git clone https://github.com/noxdafox/rabbitmq-message-deduplication.git
        cd rabbitmq-message-deduplication
        make
```

Then copy all the *.ez files inside the plugins folder to the RabbitMQ plugins directory and enable the plugin:

```bash
        [sudo] rabbitmq-plugins enable rabbitmq_message_deduplication_exchange
```

Declare an exchange
-------------------

To create a message deduplication exchange, just declare it providing the type `x-message-deduplication`.

Extra arguments:

  * `x-cache-size`: maximum size for the deduplication cache.
    This parameter is mandatory.
  * `x-cache-ttl`: amount of time in seconds messages are kept in cache.
    This parameter is optional.

Message deduplication
---------------------

Each message containing the `x-deduplication-header` header will not be routed if its value has been already submitted previously and has not expired.

The optional header `x-cache-ttl` will override the default one if provided during the exchange declaration. This parameter controls for how many seconds to deduplicate the message. After the TTL expires, a new message with the same `x-deduplication-header` header will be routed again.

License
-------

See LICENSE.txt
