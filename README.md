# fanout

This is part of nanoservice core.

## fanout daemon

Listens to specific topic on Bus and fans out messages to clients in round-robin fashion.

Enables canary deployment.

It uses Apache Kafka as a Bus.

## Roadmap

 * [ ] subscribe to one topic
   * [x] subscribe to test topic
   * [x] subscribe to all partitions of this topic
   * [ ] subscribe to topic provided by commandline argument
   * [x] listen to multiple kafkas (up to 3)
   * [ ] detect count of available kafkas and listen to all of them
 * [ ] fanout to the clients
   * [x] fanout everything to all clients
   * [x] balance messages for clients
   * [x] send message to other client if there is no ack
   * [x] buffer consumed messages if there are no clients and try to re-send as
     soon a there are
   * [ ] stop consuming the topic if there are no clients
   * [ ] maintain the offset per each partition in zookeeper
   * [ ] start consuming from saved offset per each partition instead of newest
   * [ ] connect to up to 3 zookeepers
   * [ ] detect count of available zookeepers and connect to all of them
 * [ ] durability
   * [ ] add heartbeats on special topic `fanout_service_x_#{topic_name}`
   * [ ] vote on master when there are no heartbeats from current leader
   * [ ] non-leaders return redirect-type response for clients pointing to
     leader
 * [ ] performance
   * [ ] setup benchmarks
   * [ ] implement example nanoservices to use fanout
   * [ ] adjust implementation to add minimal overhead possible
 * [ ] consistency
   * [ ] be at least "Eventually consistent":
     * [ ] one event SHOULD be received by client at least once
     * [ ] one event COULD be received by client more than once
 * [ ] logging
   * [ ] use standard logging library instead of fmt
   * [ ] `--verbose` option for showing logs with `severity > INFO`
 * [ ] testing
   * [x] basic integration tests
   * [ ] special case integration tests (like network partition, for example)
   * [ ] refactoring + unit tests

### Usage

    # Run fanout daemon on topic `user_need` and link to one `kafka` instance
    docker run -d --link kafka:kafka --link zookeeper:zk \
      nanoservice/fanout --id service_x --topic user_need

    # Run fanout daemon with `kafka` cluster
    docker run -d --link kafka_1:kafka_1 --link kafka_2:kafka_2 --link kafka_3:kafka_3 \
      --link zookeeper_1:zk_1 --link zookeeper_2:zk_2 --link zookeeper_3:zk_3 \
      nanoservice/fanout --id service_x --topic user_need

To run fanout cluster, just start multiple fanout instances with the same `--id service_x`.

Make sure that both `user_need` and `fanout_service_x_user_need` topics exist or topic auto-creation is enabled in kafka.

## go fanout client

### Usage

TODO

## List of other clients

TODO

## Development

To setup integration tests:

* If you use docker-machine + virtualbox, run this: `ln -s .virtualbox .fanout`
* If you docker directly, run this: `ln -s .docker .fanout`

After that you can run `bin/integration` to verify it works

* `bin/deps` to install all dependencies
* `bin/test` to run all unit tests
* `bin/integration` to run all unit+integration tests
* `bin/protobufs` to re-generate protobufs
* `bin/compose` to run `docker-compose` commands with right `docker-compose.yml` file

Use normal TDD development style.

## Contributing

1. Fork it ( https://github.com/nanoservice/core-fanout/fork )
1. Create your feature branch (git checkout -b my-new-feature)
1. Commit your changes (git commit -am 'Add some feature')
1. Push to the branch (git push origin my-new-feature)
1. Create a new Pull Request

## Contributors

* [waterlink](https://github.com/waterlink) Oleksii Fedorov, creator, maintainer
