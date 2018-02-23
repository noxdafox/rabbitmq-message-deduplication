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

  * `x-cache-limit`: maximum size for the deduplication cache.
    This parameter is mandatory.
  * `x-cache-ttl`: amount of time in seconds messages are kept in cache.
    This parameter is optional.

Message deduplication
---------------------

Add the `x-deduplication-header` header to the message. Its value will be tested against a cache of previously seen messages. If the header value is already present in the cache and has not expired, it will not be routed.

The optional header `x-cache-ttl` will override the default value provided during the exchange declaration.

License
-------

See LICENSE.txt
