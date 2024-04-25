![](/icon.png)

# Install
Docker image:
```shell
docker pull egsam98/kafka-pipe:{tag}
```
https://hub.docker.com/r/egsam98/kafka-pipe/tags

Go module:
```shell
go get github.com/egsam98/kafka-pipe
```

# Supported classes:
| Class                      | Source     | Sink       |
|----------------------------|------------|------------|
| [pg.Source](#pgsource)     | PostgreSQL | Kafka      |
| [pg.Snapshot](#pgsnapshot) | PostgreSQL | Kafka      |
| [ch.Sink](#chsink)         | Kafka      | ClickHouse |
| [s3.Sink](#s3sink)         | Kafka      | S3         |
| [s3.Backup](#s3backup)     | S3         | Kafka      |

# Get started
To start application you need to pass YAML-based configuration.
Common parameters of config:
```yaml
name: "name of your connector"
class: "any class described in \"Supported classes\" section"
log:
  pretty: true
  level: "info"
```
Additional params are particular for every class.

## pg.Source
Transmits events from PostgreSQL to Kafka topics via logical replication and "pgoutput" plugin.

Config parameters

| Name                           | Required | Description                                                                                                                                                      |
|--------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| pg:skip.delete                 |          | Skip delete-events. Default is `false`                                                                                                                           |
| pg:url                         | +        | Postgres connection URL                                                                                                                                          |
| pg:publication                 | +        | Publication name                                                                                                                                                 |
| pg:slot                        | +        | Replication slot name                                                                                                                                            |
| pg:tables                      | +        | Tables list to subscribe for replication                                                                                                                         |
| pg:health.table                |          | Table name. Default is `public.pipe_health`                                                                                                                      |
| kafka:brokers                  | +        | List of Kafka brokers. Format is {hostname}:{port}                                                                                                               |
| kafka:topic:prefix             |          | Prefix for created topics. Format is {prefix}.{postgres table name}                                                                                              |
| kafka:topic:replication.factor |          | Topic replication factor. Default is `1`                                                                                                                         |
| kafka:topic:partitions         |          | Number of partitions. Default is `1`                                                                                                                             |
| kafka:topic:compression.type   |          | Compression type. Default is `producer`                                                                                                                          |
| kafka:topic:cleanup.policy     |          | Cleanup policy. Default is `delete`                                                                                                                              |
| kafka:topic:routes             |          | Dictionary of mappings between Postgres relation's regular expression and Kafka topic. Example for partitions: ^public.transaction(_\d+)?$: "public.transaction" |
| kafka:batch:size               |          | Producer's batch size. Default is `10000`                                                                                                                        |
| kafka:batch:timeout            |          | Producer's batch timeout. Default is `5s`                                                                                                                        |

## pg.Snapshot
Selects all/certain rows from tables and supplies to Kafka topics. Connector stops when all rows are produced

Config parameters

| Name                           | Required | Description                                                                                     |
|--------------------------------|----------|-------------------------------------------------------------------------------------------------|
| pg:url                         | +        | Postgres connection URL                                                                         |
| pg:tables                      | +        | Tables list to execute snapshot                                                                 |
| pg:condition                   |          | SQL condition part to select rows from table. Example: "WHERE id > 1". Default is empty string. |
| kafka:brokers                  | +        | List of Kafka brokers. Format is {hostname}:{port}                                              |
| kafka:topic:prefix             | +        | Prefix for created topics. Format is {prefix}.{postgres table name}                             |
| kafka:topic:replication.factor |          | Topic replication factor. Default is `1`                                                        |
| kafka:topic:partitions         |          | Number of partitions. Default is `1`                                                            |
| kafka:topic:compression.type   |          | Compression type. Default is `producer`                                                         |
| kafka:topic:cleanup.policy     |          | Cleanup policy. Default is `delete`                                                             |
| kafka:batch:size               |          | Producer's batch size. Default is `10000`                                                       |
| kafka:batch:timeout            |          | Producer's batch timeout. Default is `5s`                                                       |

## ch.Sink
Transmits events from Kafka topics to ClickHouse. For every consumed topic group ID is created as `{connector_name}-{topic}`.

Config parameters

<table>
    <tr>
        <th>Name</th>
        <th>Required</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>kafka:brokers</td>
        <td>+</td>
        <td>List of Kafka brokers. Format is {hostname}:{port}</td>
    </tr>
    <tr>
        <td>kafka:topics</td>
        <td>+</td>
        <td>Topics to read from</td>
    </tr>
    <tr>
        <td>kafka:rebalance_timeout</td>
        <td></td>
        <td>
            How long all members are allowed to complete work and commit offsets, minus the time it took to detect the rebalance (from a heartbeat). 
            Default is <code>1m</code>
        </td>
    </tr>
    <tr>
        <td>kafka:workers_per_topic</td>
        <td></td>
        <td>Number of workers (consumers) per 1 topic. Default is <code>1</code></td>
    </tr>
    <tr>
        <td>kafka:batch:size</td>
        <td></td>
        <td>Consumer's batch size. Default is <code>10000</code></td>
    </tr>
    <tr>
        <td>kafka:batch:timeout</td>
        <td></td>
        <td>Consumer's batch timeout. Default is <code>5s</code></td>
    </tr>
    <tr>
        <td>kafka:fetch_max_bytes</td>
        <td></td>
        <td>
            Maximum amount of bytes a broker will try to send during a fetch.
            Note that brokers may not obey this limit if it has records larger than this limit. Also note that this 
            client sends a fetch to each broker concurrently, meaning the client will buffer up to brokers * max bytes worth of memory. 
            This corresponds to the Java fetch.max.bytes setting.
            Default: <code>52428800</code> (50MB)
        </td>
    </tr>
    <tr>
        <td>kafka:fetch_max_partition_bytes</td>
        <td></td>
        <td>
            Maximum amount of bytes that will be consumed for a single partition in a fetch request. 
            Note that if a single batch is larger than this number, that batch will still be returned so the client can make progress.
            This corresponds to the Java max.partition.fetch.bytes setting.
            Default: <code>1048576</code> (1MB)
        </td>
    </tr>
    <tr>
        <td>click_house:addrs</td>
        <td>+</td>
        <td>ClickHouse addresses list to connect</td>
    </tr>
    <tr>
        <td>click_house:database</td>
        <td>+</td>
        <td>Database name</td>
    </tr>
    <tr>
        <td>click_house:user</td>
        <td>+</td>
        <td>Username credential</td>
    </tr>
    <tr>
        <td>click_house:password</td>
        <td></td>
        <td>Password credential</td>
    </tr>
    <tr>
        <td>serde:format</td>
        <td></td>
        <td>
            Serde's format for Kafka messages (key & value). Default is <code>json</code>. 
            Additional settings for every Serde format are described <a href="#serde">here</a>
        </td>
    </tr>
</table>

Non-YAML configuration (available if you use connector as go-module):

```go 
func BeforeInsert(ctx context.Context, batch []*kgo.Record) error
```
Hook function that's called per every consumed batch before inserting one into ClickHouse. By reasons of reducing extra 
memory allocations you want to directly modify the slice of records. If you want to exclude(filter) particular records from batch 
you can erase them by index, i.e.
```go 
batch[i] = nil
```

## s3.Sink
No description

## s3.Backup
No description

## Serde
<p id="serde"></p>
i.e. Serialization and Deserialization.
Supported formats: `json`, `avro`.
Depending on the selected format, different settings are provided:

### json
| Name        | Required | Description                                                                                                  |
|-------------|----------|--------------------------------------------------------------------------------------------------------------|
| time_format |          | Datetime formatter, possible values: `rfc3339`, `timestamp`, `timestamp-milli`. Default is `timestamp-milli` |

### avro
| Name       | Required | Description                                                                                                                                     |
|------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| schema_uri | +        | Avro schema's source URI. Supported HTTP schemas: `file` (local download), `http(-s)` (download via HTTP protocol). Example: file://schema.avro |

Example of Serde configuration:
```yaml
serde:
  format: json
  time_format: rfc3339
```
