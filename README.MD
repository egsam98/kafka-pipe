![](/static/icon.png)
# Overview
The project is highly inspired by Kafka Connect, but implemented with Go.
The project contains several connector classes designed for Change-Data-Capture (CDC) pipeline from specific source to sink. 
Some of them use internal storage based on Badger embedded database to keep offsets/log positions/other important metadata to prevent
from data loss or duplication on connection issues. Internal storage is organized in "data" folder in a workspace of running app.

Keep in my mind to not mix connectors in common workspace unless they have unique names ("name" parameter in YAML configuration).
It's not recommended to recreate Kafka topics and use the same state of connector's internal storage.
If you have to then you might want to remove "data" folder also, but...

![consider_your_position_carefully.gif](static/consider_your_position_carefully.gif)

# Installation
Docker image:
```shell
docker pull egsam98/kafka-pipe:{tag}
```
https://hub.docker.com/r/egsam98/kafka-pipe/tags

Go module:
```shell
go get github.com/egsam98/kafka-pipe
```

# Supported connector classes:
| Class                      | Source     | Sink       |
|----------------------------|------------|------------|
| [pg.Source](#pgsource)     | PostgreSQL | Kafka      |
| [pg.Snapshot](#pgsnapshot) | PostgreSQL | Kafka      |
| [ch.Sink](#chsink)         | Kafka      | ClickHouse |
| [s3.Sink](#s3sink)         | Kafka      | S3         |
| [s3.Backup](#s3backup)     | S3         | Kafka      |

# Get started
To start application you want to pass YAML configuration as an argument for application.
```sh
./{kafka-pipe bin} config.yaml
```
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
Postgres' last read LSN is stored on disk of internal storage.

Config parameters

| Name                            | Type         | Required | Description                                                                                                                                 |
|---------------------------------|--------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| pg:skip.delete                  | Bool         |          | Skip delete-events. Default is `false`                                                                                                      |
| pg:url                          | String       | +        | Postgres connection URL                                                                                                                     |
| pg:publication                  | String       | +        | Publication name                                                                                                                            |
| pg:slot                         | String       | +        | Replication slot name                                                                                                                       |
| pg:tables                       | List(String) | +        | Tables list to subscribe for replication                                                                                                    |
| pg:health.table                 | String       |          | Table name. Default is `public.pipe_health`                                                                                                 |
| kafka:brokers                   | List(String) | +        | List of Kafka brokers. Format is {hostname}:{port}                                                                                          |
| kafka:topic:prefix              | String       |          | Prefix for created topics. Format is {prefix}.{postgres table name}                                                                         |
| kafka:topic:replication.factor  | UInt         |          | Topic replication factor. Default is `1`                                                                                                    |
| kafka:topic:partitions          | UInt         |          | Number of partitions. Default is `1`                                                                                                        |
| kafka:topic:compression.type    | String       |          | Compression type. Default is `producer`                                                                                                     |
| kafka:topic:cleanup.policy      | String       |          | Cleanup policy. Default is `delete`                                                                                                         |
| kafka:topic:retention           | String       |          | Retention duration. Default is `168h` (7 days)                                                                                              |
| kafka:topic:part_retention_size | String       |          | Partition retention size in bytes, kilobytes, megabytes or terabytes. Default: `10GB`                                                       |                                                                       |
| kafka:topic:routes              | Dict         |          | Mapping of Postgres relation's regular expression to Kafka topic. Example for partitions: ^public.transaction(_\d+)?$: "public.transaction" |
| kafka:batch:size                | UInt         |          | Producer's batch size. Default is `10000`                                                                                                   |
| kafka:batch:timeout             | String       |          | Producer's batch timeout. Default is `5s`                                                                                                   |

## pg.Snapshot
Selects all/certain rows from tables and supplies to Kafka topics. Connector stops when all rows are produced

Config parameters

| Name                            | Type         | Required | Description                                                                                                                                 |
|---------------------------------|--------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| pg:url                          | String       | +        | Postgres connection URL                                                                                                                     |
| pg:tables                       | List(String) | +        | Tables list to execute snapshot                                                                                                             |
| pg:condition                    | String       |          | SQL condition part to select rows from table. Example: "WHERE id > 1". Default is empty string.                                             |
| kafka:brokers                   | List(String) | +        | List of Kafka brokers. Format is {hostname}:{port}                                                                                          |
| kafka:topic:prefix              | String       |          | Prefix for created topics. Format is {prefix}.{postgres table name}                                                                         |
| kafka:topic:replication.factor  | UInt         |          | Topic replication factor. Default is `1`                                                                                                    |
| kafka:topic:partitions          | UInt         |          | Number of partitions. Default is `1`                                                                                                        |
| kafka:topic:compression.type    | String       |          | Compression type. Default is `producer`                                                                                                     |
| kafka:topic:cleanup.policy      | String       |          | Cleanup policy. Default is `delete`                                                                                                         |
| kafka:topic:retention           | String       |          | Retention duration. Default is `168h` (7 days)                                                                                              |
| kafka:topic:part_retention_size | String       |          | Partition retention size in bytes, kilobytes, megabytes or terabytes. Default: `10GB`                                                       |                                                                       |
| kafka:topic:routes              | Dict         |          | Mapping of Postgres relation's regular expression to Kafka topic. Example for partitions: ^public.transaction(_\d+)?$: "public.transaction" |
| kafka:batch:size                | UInt         |          | Producer's batch size. Default is `10000`                                                                                                   |
| kafka:batch:timeout             | String       |          | Producer's batch timeout. Default is `5s`                                                                                                   |

## ch.Sink
Transmits events from Kafka topics to ClickHouse. Deduplication mechanism is designed similarly to https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md#state-machine .
Kafka partition offsets are additionally stored on disk of internal storage.
For older ClickHouse versions [JSON](https://clickhouse.com/docs/sql-reference/data-types/newjson) support is provided with unquoted `Array(UInt8)` column instead.

Config parameters
<table>
    <tr>
        <th>Name</th>
        <th>Type</th>
        <th>Required</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>kafka:group</td>
        <td>String</td>
        <td>+</td>
        <td>Group ID</td>
    </tr>
    <tr>
        <td>kafka:brokers</td>
        <td>List(String)</td>
        <td>+</td>
        <td>List of Kafka brokers. Format is {hostname}:{port}</td>
    </tr>
    <tr>
        <td>kafka:topics</td>
        <td>List(String)</td>
        <td>+</td>
        <td>Topics to read from</td>
    </tr>
    <tr>
        <td>kafka:sasl:protocol</td>
        <td>String</td>
        <td></td>
        <td>SASL protocol, additional settings <a href="#sasl">here</a></td>
    </tr>
    <tr>
        <td>kafka:rebalance_timeout</td>
        <td>String</td>
        <td></td>
        <td>
            How long all members are allowed to complete work and commit offsets, minus the time it took to detect the rebalance (from a heartbeat). 
            Default is <code>1m</code>
        </td>
    </tr>
    <tr>
        <td>kafka:workers_per_topic</td>
        <td>UInt</td>
        <td></td>
        <td>Number of workers (consumers) per 1 topic. Default is <code>1</code></td>
    </tr>
    <tr>
        <td>kafka:batch:size</td>
        <td>UInt</td>
        <td></td>
        <td>Consumer's batch size. Default is <code>10000</code></td>
    </tr>
    <tr>
        <td>kafka:batch:timeout</td>
        <td>String</td>
        <td></td>
        <td>Consumer's batch timeout. Default is <code>5s</code></td>
    </tr>
    <tr>
        <td>kafka:fetch_max_bytes</td>
        <td>UInt</td>
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
        <td>UInt</td>
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
        <td>List(String)</td>
        <td>+</td>
        <td>ClickHouse addresses list to connect</td>
    </tr>
    <tr>
        <td>click_house:database</td>
        <td>String</td>
        <td>+</td>
        <td>Database name</td>
    </tr>
    <tr>
        <td>click_house:user</td>
        <td>String</td>
        <td>+</td>
        <td>Username credential</td>
    </tr>
    <tr>
        <td>click_house:password</td>
        <td>String</td>
        <td></td>
        <td>Password credential</td>
    </tr>
    <tr>
        <td>serde:format</td>
        <td>String</td>
        <td></td>
        <td>
            Serde's format for Kafka messages (key & value). Default is <code>json</code>. 
            Additional settings for every Serde format are described <a href="#serde">here</a>
        </td>
    </tr>
    <tr>
        <td>routes</td>
        <td>Dict</td>
        <td></td>
        <td>Routes of mapping Kafka topic regular expression to ClickHouse table. Example:
<pre>
routes:
    public.transaction_\d+: transactions
</pre>
        </td>
    </tr>
</table>

Non-YAML configuration (available if you use connector as go-module):

```go 
func BeforeInsert(ctx context.Context, serde Serde, topic string, batch []*kgo.Record) ([]any, error)
```
Hook function that's called per every consumed batch before inserting one into ClickHouse. Due to specific ClickHouse 
implementations (see [here](https://github.com/ClickHouse/clickhouse-go/blob/6985077eacba9197cd3ac07bbd4cc35f6f86e0dd/conn_batch.go#L181)
and [here](https://github.com/ClickHouse/clickhouse-go/blob/6985077eacba9197cd3ac07bbd4cc35f6f86e0dd/struct_map.go#L35))
result must consist of struct pointers, i.e.
```go
func BeforeInsert(ctx context.Context, serde Serde, topic string, batch []*kgo.Record) ([]any, error) {
	// Handle batch
	// ...
	return []any{&Data{Column1: 1}, &Data{Column1: 2}}, nil
}
```

## s3.Sink
Transmits events from Kafka topics to S3-compatible storage.
This connector polls Kafka records according batch settings (see `kafka:batch:*`), groups by truncated datetime + partition and
then uploads collected data chunks into S3. The storage keys are built by following schema:
<code>{topic}/{truncated datetime}/{partition}/{min_offset in chunk}-{max_offset in chunk}.gz</code>.
If previous data chunk doesn't exceed 5MB limit it's merged with new chunk and deleted. If prev. chunk's max offset >=
new chunk's max offset, new chunk is ignored.

Config parameters
<table>
    <tr>
        <th>Name</th>
        <th>Type</th>
        <th>Required</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>kafka:group</td>
        <td>String</td>
        <td>+</td>
        <td>Group ID</td>
    </tr>
    <tr>
        <td>kafka:brokers</td>
        <td>List(String)</td>
        <td>+</td>
        <td>List of Kafka brokers. Format is {hostname}:{port}</td>
    </tr>
    <tr>
        <td>kafka:topics</td>
        <td>List(String)</td>
        <td>+</td>
        <td>Topics to read from</td>
    </tr>
    <tr>
        <td>kafka:sasl:protocol</td>
        <td>String</td>
        <td></td>
        <td>SASL protocol, additional settings <a href="#sasl">here</a></td>
    </tr>
    <tr>
        <td>kafka:rebalance_timeout</td>
        <td>String</td>
        <td></td>
        <td>
            How long all members are allowed to complete work and commit offsets, minus the time it took to detect the rebalance (from a heartbeat). 
            Default is <code>1m</code>
        </td>
    </tr>
    <tr>
        <td>kafka:workers_per_topic</td>
        <td>UInt</td>
        <td></td>
        <td>Number of workers (consumers) per 1 topic. Default is <code>1</code></td>
    </tr>
    <tr>
        <td>kafka:batch:size</td>
        <td>UInt</td>
        <td></td>
        <td>Consumer's batch size. Default is <code>10000</code></td>
    </tr>
    <tr>
        <td>kafka:batch:timeout</td>
        <td>String</td>
        <td></td>
        <td>Consumer's batch timeout. Default is <code>5s</code></td>
    </tr>
    <tr>
        <td>kafka:fetch_max_bytes</td>
        <td>UInt</td>
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
        <td>UInt</td>
        <td></td>
        <td>
            Maximum amount of bytes that will be consumed for a single partition in a fetch request. 
            Note that if a single batch is larger than this number, that batch will still be returned so the client can make progress.
            This corresponds to the Java max.partition.fetch.bytes setting.
            Default: <code>1048576</code> (1MB)
        </td>
    </tr>
    <tr>
        <td>s3:endpoint</td>
        <td>String</td>
        <td>+</td>
        <td>Connection endpoint</td>
    </tr>
    <tr>
        <td>s3:bucket</td>
        <td>String</td>
        <td>+</td>
        <td>Bucket name</td>
    </tr>
    <tr>
        <td>s3:id</td>
        <td>String</td>
        <td>+</td>
        <td>Access key ID (v4)</td>
    </tr>
    <tr>
        <td>s3:secret</td>
        <td>String</td>
        <td>+</td>
        <td>Secret access key (V4)</td>
    </tr>
    <tr>
        <td>s3:ssl</td>
        <td>Bool</td>
        <td></td>
        <td>use SSL connection</td>
    </tr>
    <tr>
        <td>group_time_interval</td>
        <td>String</td>
        <td></td>
        <td>Describes how records' datetime is rounded down and collected into groups in S3. Default: <code>1h</code></td>
    </tr>
</table>

## s3.Backup
Restores Kafka messages from S3 storage back to topics. The connector stops when all selected objects are handled.
Read offsets are saved to internal storage and skipped on rescan afterwards.

Config parameters:
<table>
    <tr>
        <th>Name</th>
        <th>Type</th>
        <th>Required</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>kafka:brokers</td>
        <td>List(String)</td>
        <td>+</td>
        <td>List of Kafka brokers. Format is {hostname}:{port}</td>
    </tr>
    <tr>
        <td>kafka:batch:size</td>
        <td>UInt</td>
        <td></td>
        <td>Consumer's batch size. Default is <code>10000</code></td>
    </tr>
    <tr>
        <td>kafka:batch:timeout</td>
        <td>String</td>
        <td></td>
        <td>Consumer's batch timeout. Default is <code>0</code>. Equivalent to producer's linger</td>
    </tr>
    <tr>
        <td>s3:endpoint</td>
        <td>String</td>
        <td>+</td>
        <td>Connection endpoint</td>
    </tr>
    <tr>
        <td>s3:bucket</td>
        <td>String</td>
        <td>+</td>
        <td>Bucket name</td>
    </tr>
    <tr>
        <td>s3:id</td>
        <td>String</td>
        <td>+</td>
        <td>Access key ID (v4)</td>
    </tr>
    <tr>
        <td>s3:secret</td>
        <td>String</td>
        <td>+</td>
        <td>Secret access key (V4)</td>
    </tr>
    <tr>
        <td>s3:ssl</td>
        <td>Bool</td>
        <td></td>
        <td>use SSL connection</td>
    </tr>
    <tr>
        <td>topics</td>
        <td>List(String)</td>
        <td>+</td>
        <td>Topics to read from</td>
    </tr>
    <tr>
        <td>date_since</td>
        <td>String</td>
        <td>+</td>
        <td>UTC date from the beginning of which to select objects. Format: "2006-01-02 15:04:05"</td>
    </tr>
    <tr>
        <td>date_to</td>
        <td>String</td>
        <td>+</td>
        <td>UTC date by which to select objects. Format: "2006-01-02 15:04:05"</td>
    </tr>
</table>

## SASL
Specific settings for every SASL protocol. All params are optional.
### plain
| Name | Type   | Required | Description                               |
|------|--------|----------|-------------------------------------------|
| user | String | +        | Username                                  |
| pass | String | +        | Password                                  |
| zid  | String |          | Authorization ID to use in authenticating |
### scram-256
| Name     | Type   | Required | Description                                                                                                            |
|----------|--------|----------|------------------------------------------------------------------------------------------------------------------------|
| user     | String | +        | Username                                                                                                               |
| pass     | String | +        | Password                                                                                                               |
| zid      | String |          | Authorization ID to use in authenticating                                                                              |
| nonce    | String |          | If provided, is the string to use for authentication. Otherwise, this package uses 20 bytes read with crypto/rand (Go) |
| is_token | Bool   |          | Suffixes the "tokenauth=true" extra attribute to initial authentication message                                        |
### scram-512
See scram-256
### oath
| Name  | Type   | Required         | Description                                                    |
|-------|--------|------------------|----------------------------------------------------------------|
| token | String | +                | Oauthbearer token to use for a single session's authentication |
| zid   | String | Authorization ID |
### aws
| Name          | Type   | Required | Description                                                                                                                                                                                |
|---------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| access_key    | String | +        | AWS access key                                                                                                                                                                             |
| secret_key    | String | +        | AWS secret key                                                                                                                                                                             |
| session_token | String |          | Read more https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html                                                                                                                 |
| user_agent    | String |          | Override the default "franz-go/<runtime.Version()>/<hostname>". Read more https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-useragent |

## Serde
i.e. Serialization and Deserialization.
Supported formats: `json`, `avro`.
Depending on the selected format, different settings are provided:

### json
| Name        | Type   | Required | Description                                                                                                  |
|-------------|--------|----------|--------------------------------------------------------------------------------------------------------------|
| time_format | String |          | Datetime formatter, possible values: `rfc3339`, `timestamp`, `timestamp-milli`. Default is `timestamp-milli` |

### avro
| Name    | Type | Required | Description                                                                                                                                                                     |
|---------|------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| schemas | Dict | +        | Mapping of Kafka topics to Avro schema's source URLs. Supported HTTP schemas: `file` (local download), `http(-s)` (download via HTTP protocol). Example URL: file://schema.avsc |

Examples of Serde configuration:
```yaml
serde:
  format: json
  time_format: rfc3339
```

```yaml
serde:
  format: avro
  schemas:
    topic_test: file://schema.avsc
```
