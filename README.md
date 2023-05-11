Sample Kafka Connect Custom SMT to transform logs ingested via a source connector. Based on https://github.com/cjmatta/kafka-connect-insert-uuid/tree/master.

Example on how to add to your connector (here using FileStreamSource connector with logs available as plain text):
```
curl -X PUT \
     -H "Content-Type: application/json" \
     --data '{
               "tasks.max": "1",
               "connector.class": "FileStreamSource",
               "topic": "filestream-1",
               "file": "/home/appuser/input/logs.txt",
               "key.converter": "org.apache.kafka.connect.storage.StringConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.storage.StringConverter",
               "value.converter.schemas.enable": "false",
               "errors.tolerance": "all",
               "errors.log.enable": "true",
               "errors.log.include.messages": "true",
               "transforms": "HoistField,logstransformer",
               "transforms.HoistField.type": "org.apache.kafka.connect.transforms.HoistField$Value",
               "transforms.HoistField.field": "message",
               "transforms.logstransformer.type": "io.confluent.kafka.connect.smt.LogsTransformer$Value",
               "transforms.logstransformer.index.name": "brnachssb_sshk_pcf_logs"
          }' \
     http://localhost:8083/connectors/filestream-source-1/config | jq .
```

Supports only plain text raw logs for now.
