package io.confluent.kafka.connect.datagen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Iterator;
import java.util.Map;

public class JsonSchemaGenerator implements MessageGenerator {
    
    private static final String JSON_DOCUMENT = 
        "{\"id\": 12345, \"name\": \"Sample User\", \"timestamp\": 1640995200000}";
    
    private static final String JSON_KEY = 
        "{\"userId\": 12345}";
    
    private final ObjectMapper objectMapper;
    private int counter = 0;    
    
    public JsonSchemaGenerator() {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public Message generate() {
        try {
            // Parse the JSON string into JsonNode
            JsonNode jsonNode = objectMapper.readTree(JSON_DOCUMENT);
            JsonNode keyNode = objectMapper.readTree(JSON_KEY);
            
            // Convert JSON to Schema and Value manually
            SchemaAndValue valueSchemaAndValue = convertJsonNodeToSchemaAndValue(jsonNode);
            SchemaAndValue keySchemaAndValue = new SchemaAndValue(
                Schema.STRING_SCHEMA, "Sample User" + counter
            );
            
            return new Message(keySchemaAndValue, valueSchemaAndValue);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate JSON message", e);
        }
    }
    
    private SchemaAndValue convertJsonNodeToSchemaAndValue(JsonNode jsonNode) {
        if (jsonNode.isObject()) {
            SchemaBuilder schemaBuilder = SchemaBuilder.struct();
            
            // First pass: build the schema
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                
                if (fieldValue.isInt()) {
                    schemaBuilder.field(fieldName, Schema.INT32_SCHEMA);
                } else if (fieldValue.isLong()) {
                    schemaBuilder.field(fieldName, Schema.INT64_SCHEMA);
                } else if (fieldValue.isTextual()) {
                    schemaBuilder.field(fieldName, Schema.STRING_SCHEMA);
                } else if (fieldValue.isBoolean()) {
                    schemaBuilder.field(fieldName, Schema.BOOLEAN_SCHEMA);
                } else if (fieldValue.isDouble()) {
                    schemaBuilder.field(fieldName, Schema.FLOAT64_SCHEMA);
                }
            }
            schemaBuilder.field("counter", Schema.INT32_SCHEMA);    
            
            // Build the final schema
            Schema schema = schemaBuilder.build();
            
            // Second pass: create the Struct with the built schema
            Struct struct = new Struct(schema);
            fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                
                if (fieldValue.isInt()) {
                    struct.put(fieldName, fieldValue.asInt());
                } else if (fieldValue.isLong()) {
                    struct.put(fieldName, fieldValue.asLong());
                } else if (fieldValue.isTextual()) {
                    struct.put(fieldName, fieldValue.asText());
                } else if (fieldValue.isBoolean()) {
                    struct.put(fieldName, fieldValue.asBoolean());
                } else if (fieldValue.isDouble()) {
                    struct.put(fieldName, fieldValue.asDouble());
                }
            }
            
            counter++;
            struct.put("counter", counter);
            return new SchemaAndValue(schema, struct);
        }
        
        throw new UnsupportedOperationException("Only JSON objects are supported");
    }
}
