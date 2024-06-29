# RabbitMQ Message Deduplication Plugin

[![Build Status](https://github.com/noxdafox/rabbitmq-message-deduplication/actions/workflows/action.yml/badge.svg)](https://github.com/noxdafox/rabbitmq-message-deduplication/actions/workflows/action.yml)

A plugin for filtering duplicate messages.

Messages can be deduplicated when published into an exchange or enqueued to a queue.

## Installing

Download the `.ez` files from the chosen [release](https://github.com/noxdafox/rabbitmq-message-deduplication/releases) and copy them into the [RabbitMQ plugins directory](http://www.rabbitmq.com/relocate.html).

Check the Release notes for minimum supported versions.

Enable the plugin:

```bash
    [sudo] rabbitmq-plugins enable rabbitmq_message_deduplication
```

## Building from Source

Please see RabbitMQ Plugin Development guide.

To build the plugin:

```bash
    git clone https://github.com/noxdafox/rabbitmq-message-deduplication.git
    cd rabbitmq-message-deduplication
    make dist
```

Then copy all the *.ez files inside the plugins folder to the [RabbitMQ plugins directory](http://www.rabbitmq.com/relocate.html) and enable the plugin:

```bash
    [sudo] rabbitmq-plugins enable rabbitmq_message_deduplication
```

## Version requirements

The latest version of the plugin requires RabbitMQ 3.13.0.

Earlier RabbitMQ versions are supported by 0.6.2.

## Exchange level deduplication

The exchange type `x-message-deduplication` allows to filter message duplicates before any routing rule is applied.

Each message containing the `x-deduplication-header` header will not be routed if its value has been submitted previously. The amount of time a given message will be guaranteed to be unique can be controlled via the `x-cache-ttl` exchange argument or message header.

> **_NOTE;_** This exchange acts like a [`fanout` exchange](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-fanout), so routing rules are not applied.

### Declare an exchange

To create a message deduplication exchange, just declare it providing the type `x-message-deduplication`.

Required arguments:

  * `x-cache-size`: maximum number of entries for the deduplication cache. If the deduplication cache fills up, unspecified existing entries will be removed to give space to new ones.

Optional arguments:

  * `x-cache-ttl`: amount of time in milliseconds duplicate headers are kept in cache.
  * `x-cache-persistence`: whether the duplicates cache will persist on disk or in memory.
    Default persistence type is `memory`.

### Message headers

  * `x-deduplication-header`: messages will be deduplicated based on the content of this header. If the header is not provided, the message will not be checked against duplicates.
  * `x-cache-ttl`: this header is optional and will override the default value provided during the exchange declaration. This header controls for how many milliseconds to deduplicate the message. After the TTL expires, a new message with the same header will be routed again.

## Queue level deduplication

A queue declared with the `x-message-deduplication` parameter enabled will filter message duplicates before they are published within.

Each message containing the `x-deduplication-header` header will not be enqueued if another message with the same header is already present within the queue.

> **_NOTE:_**  Mirrored and Quorum queues are currently not supported.

### Declare a queue

When declaring a queue, it is possible to enable message deduplication via the `x-message-deduplication` boolean argument.

### Message headers

  * `x-deduplication-header`: messages will be deduplicated based on the content of this header. If the header is not provided, the message will not be checked against duplicates.

## Disabling the Plugin

It is possible to disable the plugin via the command:

```bash
    [sudo] rabbitmq-plugins disable rabbitmq_message_deduplication
```

All deduplication exchanges and queues will be rendered non functional. It is responsibility of the User to remove them.

## Running the tests

```bash
    make tests
```

## License

See the LICENSE file.
