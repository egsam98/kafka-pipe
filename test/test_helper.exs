Testcontainers.start_link()

Mimic.copy(:brod)
Mimic.copy(KafkaPipe.Connector.Source.Postgres.Internal)
Mimic.copy(KafkaPipe.Connector.MemberDB)

ExUnit.start()
