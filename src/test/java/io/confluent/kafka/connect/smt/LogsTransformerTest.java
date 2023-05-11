package io.confluent.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class LogsTransformerTest {

  private LogsTransformer<SourceRecord> xform = new LogsTransformer.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }
  
  @Test
  public void schemaTransformField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("index.name", "brnachssb_sshk_pcf_logs");

    xform.configure(props);

    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("message", Schema.OPTIONAL_STRING_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("message", "1287 <14>1 2023-04-26T05:55:26.695883+00:00 sshk.uat.eformservice-sshk-green 3517892b-f358-4da9-9282-e149eaa8dc96 "
			+ "[APP/PROC/WEB/0] - [tags@47450 app_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" app_name=\"eformservice-sshk-green\" "
			+ "deployment=\"p-isolation-segment-sharedext-5616e12099b1098d7322\" index=\"f57b6394-c652-42ff-99b5-9fb25381e6e9\" "
			+ "instance_id=\"0\" ip=\"10.52.32.70\" job=\"isolated_diego_cell_sharedext\" "
			+ "organization_id=\"3d6d325e-6225-4c90-a291-48a64c46ef49\" organization_name=\"sshk\" origin=\"rep\" "
			+ "placement_tag=\"isolation-segment-sharedext\" process_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" "
			+ "process_instance_id=\"3cc31400-e841-407e-5a5b-65a3\" process_type=\"web\" product=\"Isolation Segment\" "
			+ "source_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" source_type=\"APP/PROC/WEB\" "
			+ "space_id=\"690931c1-6427-4ad5-983b-9770744e6092\" space_name=\"uat\" "
			+ "system_domain=\"dev.sys.cs.domain\"] 2023-04-26 13:55:26.694 [http-nio-8080-exec-9] "
			+ "INFO c.d.s.e.handler.AuditTrailLogFilter.writeGrafanaLog:166 - [generateEvoucher20230426135525320131] "
			+ "{\"timestamp\":\"2023-04-26 13:55:26.694\",\"serviceID\":\"eform-service\","
			+ "\"functionalMap\":\"EFORM_CREATE\",\"type\":\"sshk_eform-service\",\"requestType\":\"POST\","
			+ "\"msguid\":\"d155c8b3-74e5-41e5-b703-bd5beb6732f6\",\"clientIp\":\"11.29.227.50\",\"statusCode\":201,"
			+ "\"executionTimeMilliseconds\":1282}");

    final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
    final SourceRecord transformedRecord = xform.apply(record);
    
    assertNotNull(((Map) transformedRecord.value()));
    assertNotNull(((Map) transformedRecord.value()).get("_index"));
    assertEquals(((Map) transformedRecord.value()).get("_index"), props.get("index.name"));
  }

  @Test
  public void schemalessTransformField() {
	final Map<String, Object> props = new HashMap<>();

	props.put("index.name", "brnachssb_sshk_pcf_logs");

	xform.configure(props);
	
	Map<String, Object> testRecord = new HashMap<String, Object>();
	testRecord.put("message", "1287 <14>1 2023-04-26T05:55:26.695883+00:00 sshk.uat.eformservice-sshk-green 3517892b-f358-4da9-9282-e149eaa8dc96 "
			+ "[APP/PROC/WEB/0] - [tags@47450 app_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" app_name=\"eformservice-sshk-green\" "
			+ "deployment=\"p-isolation-segment-sharedext-5616e12099b1098d7322\" index=\"f57b6394-c652-42ff-99b5-9fb25381e6e9\" "
			+ "instance_id=\"0\" ip=\"10.52.32.70\" job=\"isolated_diego_cell_sharedext\" "
			+ "organization_id=\"3d6d325e-6225-4c90-a291-48a64c46ef49\" organization_name=\"sshk\" origin=\"rep\" "
			+ "placement_tag=\"isolation-segment-sharedext\" process_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" "
			+ "process_instance_id=\"3cc31400-e841-407e-5a5b-65a3\" process_type=\"web\" product=\"Isolation Segment\" "
			+ "source_id=\"3517892b-f358-4da9-9282-e149eaa8dc96\" source_type=\"APP/PROC/WEB\" "
			+ "space_id=\"690931c1-6427-4ad5-983b-9770744e6092\" space_name=\"uat\" "
			+ "system_domain=\"dev.sys.cs.domain\"] 2023-04-26 13:55:26.694 [http-nio-8080-exec-9] "
			+ "INFO c.d.s.e.handler.AuditTrailLogFilter.writeGrafanaLog:166 - [generateEvoucher20230426135525320131] "
			+ "{\"timestamp\":\"2023-04-26 13:55:26.694\",\"serviceID\":\"eform-service\","
			+ "\"functionalMap\":\"EFORM_CREATE\",\"type\":\"sshk_eform-service\",\"requestType\":\"POST\","
			+ "\"msguid\":\"d155c8b3-74e5-41e5-b703-bd5beb6732f6\",\"clientIp\":\"11.29.227.50\",\"statusCode\":201,"
			+ "\"executionTimeMilliseconds\":1282}");

	final SourceRecord record = new SourceRecord(null, null, "test", 0, null, testRecord);
    final SourceRecord transformedRecord = xform.apply(record);
    
    assertNotNull(((Map) transformedRecord.value()));
    assertNotNull(((Map) transformedRecord.value()).get("_index"));
    assertEquals(((Map) transformedRecord.value()).get("_index"), props.get("index.name"));
  }
}
