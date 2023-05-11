package io.confluent.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class LogsTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Do everything that Logstash does";

  private interface ConfigName {
    String INDEX_NAME = "index.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.INDEX_NAME, ConfigDef.Type.STRING, "index.name", ConfigDef.Importance.HIGH,
      "Field name for INDEX");

  private static final String PURPOSE = "adding INDEX name to record";

  private String fieldName;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(ConfigName.INDEX_NAME);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }
  
  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }
  
  private Map<String, Object> transformLogs(final String message) {

	  String split[] = message.split("ip=|\"clientIp\":|\"serviceID\":|\"functionalMap\":");
		
	  String host = split[1].split(" ")[0].replace("\"", "");
	  String clientIp = split[4].split(",")[0].replace("\"", "");
	  String serviceID = split[2].split(" ")[0].replace("\"", "").replace(",", "");
	  String functionalMap = split[3].split(",")[0].replace("\"", "");
		
	  Map<String, Object> updatedRecord = new HashMap<String, Object>();
		
	  Map<String, Object> _source = new HashMap<String, Object>();
	  _source.put("host", host);
	  _source.put("clientIp", clientIp);
	  _source.put("serviceID", serviceID);
	  _source.put("functionalMap", functionalMap);
	  _source.put("message", message);
		
	  updatedRecord.put("_source", _source);
	  updatedRecord.put("_type", "_doc");
	  updatedRecord.put("_version", 1);
	  updatedRecord.put("_score", 1);
		
	  updatedRecord.put("_index", fieldName);
	  
	  return updatedRecord;
  }

  private R applySchemaless(R record) {
	
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
    
    String message = (String) value.get("message");
	
	Map<String, Object> updatedRecord = transformLogs(message);

    return newRecord(record, null, updatedRecord);
  }

  private R applyWithSchema(R record) {
	
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    
    String message = (String) value.get("message");
    
    Map<String, Object> updatedRecord = transformLogs(message);

    return newRecord(record, null, updatedRecord);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends LogsTransformer<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends LogsTransformer<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
}
