Sample Kafka Connect Custom SMT to transform logs ingested via a source connector. Based on https://github.com/cjmatta/kafka-connect-insert-uuid/tree/master.

Example on how to add to your connector:
```
transforms=logstransformer
transforms.logstransformer.type=io.confluent.kafka.connect.smt.LogsTransformer$Value
transforms.logstransformer.index.name="brnachssb_sshk_pcf_logs"
```

Supports only plain text raw logs for now.