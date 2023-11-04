# Supported classes:
| Class                      | Source     | Sink  |
|----------------------------|------------|-------|
| [pg.Source](#pgsource)     | PostgreSQL | Kafka |
| [pg.Snapshot](#pgsnapshot) | PostgreSQL | Kafka |
| [s3.Sink](#s3sink)         | Kafka      | S3    |
| [s3.Backup](#s3backup)     | S3         | Kafka | 

# Overview
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

| Name                           | Required | Description                                                         |
|--------------------------------|----------|---------------------------------------------------------------------|
| pg:skip.delete                 |          | Skip delete-events. Default is `false`                              |
| pg:url                         | +        | Postgres connection URL                                             |
| pg:publication                 | +        | Publication name                                                    |
| pg:slot                        | +        | Replication slot name                                               |
| pg:tables                      | +        | Tables list to subscribe for replication                            |
| pg:health.table                |          | Table name. Default is `public.pipe_health`                         |
| kafka:brokers                  | +        | List of Kafka brokers. Format is {hostname}:{port}                  |
| kafka:topic:prefix             | +        | Prefix for created topics. Format is {prefix}.{postgres table name} |
| kafka:topic:replication.factor |          | Topic replication factor. Default is `1`                            |
| kafka:topic:partitions         |          | Number of partitions. Default is `1`                                |
| kafka:topic:compression.type   |          | Compression type. Default is `producer`                             |
| kafka:topic:cleanup.policy     |          | Cleanup policy. Default is `delete`                                 |
| kafka:batch:size               |          | Producer's batch size. Default is `10000`                           |
| kafka:batch:timeout            |          | Producer's batch timeout. Default is `5s`                           |

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

## s3.Sink
No description

## s3.Backup
No description

