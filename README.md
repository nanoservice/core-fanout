# fanout

This is part of nanoservice core.

## fanout daemon

Listens to specific topic on Bus and fans out messages to clients in round-robin fashion.

Enables canary deployment.

It uses Apache Kafka as a Bus.

### Usage

    # Run fanout daemon on topic `user_need` and link to one `kafka` instance
    docker run -d --link kafka:kafka \
      nanoservice/fanout --id service_x --topic user_need

    # Run fanout daemon with `kafka` cluster
    docker run -d --link kafka_1:kafka_1 --link kafka_2:kafka_2 --link kafka_3:kafka_3 \
      nanoservice/fanout --id service_x --topic user_need

To run fanout cluster, just start multiple fanout instances with the same `--id service_x`.

Make sure that both `user_need` and `fanout_service_x_user_need` topics exist or topic auto-creation is enabled in kafka.

## go fanout client

### Usage

TODO

## List of other clients

TODO

## Development

* `bin/deps` to install all dependencies
* `bin/test` to run all unit tests
* `bin/integration` to run all unit+integration tests
* `bin/protobufs` to re-generate protobufs

Use normal TDD development style.

## Contributing

1. Fork it ( https://github.com/nanoservice/core-fanout/fork )
1. Create your feature branch (git checkout -b my-new-feature)
1. Commit your changes (git commit -am 'Add some feature')
1. Push to the branch (git push origin my-new-feature)
1. Create a new Pull Request

## Contributors

* [waterlink](https://github.com/waterlink) Oleksii Fedorov, creator, maintainer
