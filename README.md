# kafka-versioned-topics

A convenient way to mass manage different versions of the same topic.

## Config

The configuration is a YAML file specifying the template for each topic:

```yaml
topics:
  foo.$VERSION:
    partitions: 3
    replicas: 2
    config:
      retention.ms: 100000
  bar.$VERSION.tenant:
    partitions: 4
    replicas: 1
    config:
      cleanup.policy: compact
```

For example, if this config is used with `-version v2`, then this tool will look
at the topics `foo.v2` and `bar.v2.tenant`.

## Usage

Check the status of a Kafka cluster against your configuration:

```sh
$ java -jar kafka-versioned-topics.jar \
    -zk <zookeeper connection string> \
    -config <path to yaml> \
    -version <replacement for $VERSION> \
    status
foo.v2
  current: UnknownTopicOrPartitionException
  target:  3 partitions, 2 replicas [retention.ms=100000]
bar.v2.tenant
  current: 2 partitions, 2 replicas []
  target:  4 partitions, 1 replicas [cleanup.policy=compact]
```

Create topics for the given version:

```sh
$ java -jar kafka-versioned-topics.jar \
    -zk <zookeeper connection string> \
    -config <path to yaml> \
    -version <replacement for $VERSION> \
    -for_real \
    create
Creating topics:
-> foo.v2
Not creating topics:
-> bar.v2.tenant
```

Delete topics for the given version:

```sh
$ java -jar kafka-versioned-topics.jar \
    -zk <zookeeper connection string> \
    -config <path to yaml> \
    -version <replacement for $VERSION> \
    -for_real \
    delete
Deleting topics:
-> foo.v2
Not deleting topics:
-> bar.v2.tenant
```

Leaving off the `-for_real` switch will initiate a dry run, listing the topics
that would be created or deleted.

Note that `create` will only create topics that don't exist. Similarly, `delete`
will only delete topics that match exactly your configured specification.

## Building

SBT is configured to create a single jar with all dependencies included:

```sh
$ sbt assembly
```
