package io.confluent.kafka.connect.datagen;

import org.apache.kafka.connect.data.SchemaAndValue;

class Message {
    private SchemaAndValue key;
    private SchemaAndValue value;

    public Message(SchemaAndValue key, SchemaAndValue value) {
        this.key = key;
        this.value = value;
    }

    public SchemaAndValue getKey() {
        return key;
    }

    public SchemaAndValue getValue() {
        return value;
    }
}


public interface MessageGenerator {
    public Message generate();
}
