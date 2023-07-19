# List of connector classes:
| Class                       | Source     | Sink  |
|-----------------------------|------------|-------|
| [pg.Source](#pg.Source)     | PostgreSQL | Kafka |
| [pg.Snapshot](#pg.Snapshot) | PostgreSQL | Kafka |
| [s3.Sink](#s3.Sink)         | Kafka      | S3    |
| [s3.Backup](#s3.Backup)     | S3         | Kafka | 

# Overview
To start application you need to pass YAML-based configuration.
Common parameters of config:
```yaml
log:
  pretty: true
  level: "info"
connector:
  name: "name of your connector"
  class: "any class described in \"Supported classes\" section"
```
Additional params are particular for every class.

# Supported classes
<a id="supported-classes"></a>

## pg.Source
<a id="pg.Source"></a>
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
<a id="pg.Snapshot"></a>
Selects all/certain rows from tables and supplies to Kafka topics. Connector stops when all rows are produced

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

## s3.Sink
<a id="s3.Sink"></a>
No description

## s3.Backup
<a id="s3.Backup"></a>
No description

