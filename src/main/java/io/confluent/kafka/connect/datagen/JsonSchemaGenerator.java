package io.confluent.kafka.connect.datagen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;
import net.jimblackler.jsongenerator.Configuration;
import net.jimblackler.jsongenerator.DefaultConfig;
import net.jimblackler.jsongenerator.Generator;
import net.jimblackler.jsonschemafriend.SchemaStore;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class JsonSchemaGenerator implements MessageGenerator {
    
    private final ObjectMapper objectMapper;
    private final SchemaStore schemaStore;
    private final Generator generator;
    private final Random random;
    private net.jimblackler.jsonschemafriend.Schema schema;
    private final String schemaKeyField;
    
    public JsonSchemaGenerator(DatagenConnectorConfig config) {
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.schemaStore = new SchemaStore(true);

        this.schemaKeyField = config.getSchemaKeyfield();
        try {
            
            // Load the schema from the JSON schema string
            schema = schemaStore.loadSchemaJson(config.getJsonSchema());
            
            // Configure the generator
            Configuration jsonConfig = DefaultConfig.build()
                .setGenerateMinimal(false)
                .setNonRequiredPropertyChance(1.0f)  // Always generate all properties
                .get();
            
            this.generator = new Generator(jsonConfig, schemaStore, random);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize JsonSchemaGenerator", e);
        }
    }
    
    @Override
    public Message generate() {
        try {
            // Generate a random JSON document from the schema using jsonschemafriend
            Object generatedJson = generator.generate(schema, 1);
            
            // Convert the generated object to a JsonNode for easier processing
            JsonNode jsonNode = objectMapper.valueToTree(generatedJson);
            
            // Convert the generated JSON to Schema and Value
            SchemaAndValue valueSchemaAndValue = convertJsonNodeToSchemaAndValue(jsonNode);
            
            SchemaAndValue key = new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null);
            if (!schemaKeyField.isEmpty()) {
                JsonNode keyNode = jsonNode.get(schemaKeyField);
                if (keyNode.isInt()) {
                    key = new SchemaAndValue(Schema.INT32_SCHEMA, keyNode.asInt());
                } else if (keyNode.isLong()) {
                    key = new SchemaAndValue(Schema.INT64_SCHEMA, keyNode.asLong());
                } else if (keyNode.isTextual()) {
                    key = new SchemaAndValue(Schema.STRING_SCHEMA, keyNode.asText());
                } else {
                    throw new IllegalArgumentException("Schema key field must be a string, int, or long");
                }
            }
            
            return new Message(key, valueSchemaAndValue);
            
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
            
            return new SchemaAndValue(schema, struct);
        }
        
        throw new UnsupportedOperationException("Only JSON objects are supported");
    }
}
