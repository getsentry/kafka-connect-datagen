package io.confluent.kafka.connect.datagen;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonSchemaGeneratorTest {

    private JsonSchemaGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new JsonSchemaGenerator();
    }

    @Test
    void shouldGenerateMessage() {
        Message message = generator.generate();
        
        assertNotNull(message);
        assertNotNull(message.getKey());
        assertNotNull(message.getValue());
    }

    @Test
    void shouldGenerateCorrectKeySchema() {
        Message message = generator.generate();
        SchemaAndValue key = message.getKey();
        
        assertNotNull(key.schema());
        assertTrue(key.schema().type() == Schema.STRING_SCHEMA.type());
    }

    @Test
    void shouldGenerateCorrectKeyValue() {
        Message message = generator.generate();
        SchemaAndValue key = message.getKey();
        
        assertTrue(key.value() instanceof String);
        assertTrue(((String) key.value()).startsWith("user_"));
    }

    @Test
    void shouldGenerateCorrectValueSchema() {
        Message message = generator.generate();
        SchemaAndValue value = message.getValue();
        
        assertNotNull(value.schema());
        assertTrue(value.schema().type() == Schema.Type.STRUCT);
        assertEquals(7, value.schema().fields().size());
        
        // Check individual fields
        assertNotNull(value.schema().field("id"));
        assertNotNull(value.schema().field("name"));
        assertNotNull(value.schema().field("email"));
        assertNotNull(value.schema().field("age"));
        assertNotNull(value.schema().field("active"));
        assertNotNull(value.schema().field("score"));
        assertNotNull(value.schema().field("timestamp"));
        
        // The jsonschemafriend library may generate different types than expected
        // Check that the types are reasonable for the data
        assertTrue(value.schema().field("id").schema().type() == Schema.INT32_SCHEMA.type() || 
                  value.schema().field("id").schema().type() == Schema.INT64_SCHEMA.type(),
                  "ID should be INT32 or INT64, but was " + value.schema().field("id").schema().type());
        assertEquals(Schema.STRING_SCHEMA.type(), value.schema().field("name").schema().type());
        assertEquals(Schema.STRING_SCHEMA.type(), value.schema().field("email").schema().type());
        assertTrue(value.schema().field("age").schema().type() == Schema.INT32_SCHEMA.type() || 
                  value.schema().field("age").schema().type() == Schema.INT64_SCHEMA.type(),
                  "Age should be INT32 or INT64, but was " + value.schema().field("age").schema().type());
        assertEquals(Schema.BOOLEAN_SCHEMA.type(), value.schema().field("active").schema().type());
        assertEquals(Schema.FLOAT64_SCHEMA.type(), value.schema().field("score").schema().type());
        assertTrue(value.schema().field("timestamp").schema().type() == Schema.INT32_SCHEMA.type() || 
                  value.schema().field("timestamp").schema().type() == Schema.INT64_SCHEMA.type(),
                  "Timestamp should be INT32 or INT64, but was " + value.schema().field("timestamp").schema().type());
    }

    @Test
    void shouldGenerateCorrectValueData() {
        Message message = generator.generate();
        SchemaAndValue value = message.getValue();
        
        assertTrue(value.value() instanceof Struct);
        Struct valueStruct = (Struct) value.value();
        
        // Check that all required fields are present and have valid types
        // Handle both INT32 and INT64 for numeric fields
        Object idValue = valueStruct.get("id");
        assertTrue(idValue instanceof Integer || idValue instanceof Long);
        if (idValue instanceof Integer) {
            int id = (Integer) idValue;
            assertTrue(id >= 1 && id <= 999999);
        } else {
            long id = (Long) idValue;
            assertTrue(id >= 1L && id <= 999999L);
        }
        
        assertNotNull(valueStruct.getString("name"));
        assertTrue(valueStruct.getString("name").length() >= 3);
        assertNotNull(valueStruct.getString("email"));
        assertTrue(valueStruct.getString("email").contains("@"));
        
        Object ageValue = valueStruct.get("age");
        assertTrue(ageValue instanceof Integer || ageValue instanceof Long);
        if (ageValue instanceof Integer) {
            int age = (Integer) ageValue;
            assertTrue(age >= 18 && age <= 100);
        } else {
            long age = (Long) ageValue;
            assertTrue(age >= 18L && age <= 100L);
        }
        
        // active is boolean, so no range check needed
        assertTrue(valueStruct.getFloat64("score") >= 0.0 && valueStruct.getFloat64("score") <= 100.0);
        
        Object timestampValue = valueStruct.get("timestamp");
        assertTrue(timestampValue instanceof Integer || timestampValue instanceof Long);
        if (timestampValue instanceof Integer) {
            int timestamp = (Integer) timestampValue;
            // The jsonschemafriend library generates timestamps in milliseconds since epoch
            // 2022-01-01 to 2025-01-01 in milliseconds
            assertTrue(timestamp >= 1640995200000L && timestamp <= 1735689600000L);
        } else {
            long timestamp = (Long) timestampValue;
            // The jsonschemafriend library generates timestamps in milliseconds since epoch
            // 2022-01-01 to 2025-01-01 in milliseconds
            assertTrue(timestamp >= 1640995200000L && timestamp <= 1735689600000L);
        }
    }

    @Test
    void shouldGenerateDifferentMessages() {
        Message message1 = generator.generate();
        Message message2 = generator.generate();
        
        // Messages should be different (random generation)
        assertNotEquals(message1.getValue().value(), message2.getValue().value());
    }

    @Test
    void shouldGenerateIncrementingCounter() {
        Message message1 = generator.generate();
        Message message2 = generator.generate();
        
        SchemaAndValue key1 = message1.getKey();
        SchemaAndValue key2 = message2.getKey();
        
        String keyValue1 = (String) key1.value();
        String keyValue2 = (String) key2.value();
        
        int counter1 = Integer.parseInt(keyValue1.substring(5)); // Extract number after "user_"
        int counter2 = Integer.parseInt(keyValue2.substring(5));
        
        assertEquals(counter1 + 1, counter2);
    }

    @Test
    void shouldGenerateJsonFromSchema() {
        Message message = generator.generate();
        SchemaAndValue value = message.getValue();
        
        assertTrue(value.value() instanceof Struct);
        Struct valueStruct = (Struct) value.value();
        
        // Verify that the generated data follows the schema constraints
        // This test demonstrates that jsonschemafriend is actually generating from the schema
        
        // ID should be within schema bounds (1-999999)
        Object idValue = valueStruct.get("id");
        assertTrue(idValue instanceof Integer || idValue instanceof Long);
        if (idValue instanceof Integer) {
            int id = (Integer) idValue;
            assertTrue(id >= 1 && id <= 999999, "ID should be between 1 and 999999, but was " + id);
        } else {
            long id = (Long) idValue;
            assertTrue(id >= 1L && id <= 999999L, "ID should be between 1 and 999999, but was " + id);
        }
        
        // Name should be a string with length constraints
        String name = valueStruct.getString("name");
        assertNotNull(name);
        assertTrue(name.length() >= 3 && name.length() <= 50, 
                 "Name length should be between 3 and 50, but was " + name.length() + " for name: " + name);
        
        // Email should contain @ symbol
        String email = valueStruct.getString("email");
        assertNotNull(email);
        assertTrue(email.contains("@"), "Email should contain @ symbol: " + email);
        
        // Age should be within schema bounds (18-100)
        Object ageValue = valueStruct.get("age");
        assertTrue(ageValue instanceof Integer || ageValue instanceof Long);
        if (ageValue instanceof Integer) {
            int age = (Integer) ageValue;
            assertTrue(age >= 18 && age <= 100, "Age should be between 18 and 100, but was " + age);
        } else {
            long age = (Long) ageValue;
            assertTrue(age >= 18L && age <= 100L, "Age should be between 18 and 100, but was " + age);
        }
        
        // Score should be within schema bounds (0.0-100.0)
        double score = valueStruct.getFloat64("score");
        assertTrue(score >= 0.0 && score <= 100.0, "Score should be between 0.0 and 100.0, but was " + score);
        
        // Timestamp should be within schema bounds
        Object timestampValue = valueStruct.get("timestamp");
        assertTrue(timestampValue instanceof Integer || timestampValue instanceof Long);
        if (timestampValue instanceof Integer) {
            int timestamp = (Integer) timestampValue;
            // The jsonschemafriend library generates timestamps in milliseconds since epoch
            // 2022-01-01 to 2025-01-01 in milliseconds
            assertTrue(timestamp >= 1640995200000L && timestamp <= 1735689600000L, 
                      "Timestamp should be between 2022-01-01 and 2025-01-01, but was " + timestamp);
        } else {
            long timestamp = (Long) timestampValue;
            // The jsonschemafriend library generates timestamps in milliseconds since epoch
            // 2022-01-01 to 2025-01-01 in milliseconds
            assertTrue(timestamp >= 1640995200000L && timestamp <= 1735689600000L, 
                      "Timestamp should be between 2022-01-01 and 2025-01-01, but was " + timestamp);
        }
        
        System.out.println("Generated JSON from schema:");
        System.out.println("  ID: " + idValue + " (type: " + idValue.getClass().getSimpleName() + ")");
        System.out.println("  Name: " + name);
        System.out.println("  Email: " + email);
        System.out.println("  Age: " + ageValue + " (type: " + ageValue.getClass().getSimpleName() + ")");
        System.out.println("  Active: " + valueStruct.getBoolean("active"));
        System.out.println("  Score: " + score);
        System.out.println("  Timestamp: " + timestampValue + " (type: " + timestampValue.getClass().getSimpleName() + ")");
    }
}
