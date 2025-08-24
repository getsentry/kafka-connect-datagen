package io.confluent.kafka.connect.datagen;

import java.util.Random;

import org.apache.kafka.connect.data.Schema;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;

public class AvroGenerator implements MessageGenerator {
    private final org.apache.avro.Schema schema;
    private Generator generator;
    private String schemaKeyField;
    private AvroData avroData;
    private static final Schema DEFAULT_KEY_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

    public AvroGenerator(org.apache.avro.Schema schema, Random random, long count, DatagenConnectorConfig config) {
        this.schema = schema;

        Generator.Builder generatorBuilder = new Generator.Builder()
        .random(random)
        .generation(count)
        .schema(this.schema);

        this.generator = generatorBuilder.build();
        schemaKeyField = config.getSchemaKeyfield();
        avroData = new AvroData(1);
    }

    @Override
    public Message generate() {
        final Object generatedObject = generator.generate();
        if (!(generatedObject instanceof GenericRecord)) {
        throw new RuntimeException(String.format(
            "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
            generatedObject.getClass().getName()
        ));
        }
        final GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

        // Key
        SchemaAndValue key = new SchemaAndValue(DEFAULT_KEY_SCHEMA, null);
        if (!schemaKeyField.isEmpty()) {
            key = avroData.toConnectData(
                randomAvroMessage.getSchema().getField(schemaKeyField).schema(),
                randomAvroMessage.get(schemaKeyField)
            );
        }

        // Value
        final org.apache.kafka.connect.data.Schema messageSchema = avroData.toConnectSchema(schema);
        final Object messageValue = avroData.toConnectData(schema, randomAvroMessage).value();

        SchemaAndValue value = new SchemaAndValue(messageSchema, messageValue);
            
        return new Message(key, value);
    }
}