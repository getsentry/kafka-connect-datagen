/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.connect.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatagenTask extends SourceTask {

  static final Logger log = LoggerFactory.getLogger(DatagenTask.class);

  public static final String TASK_ID = "task.id";
  public static final String TASK_GENERATION = "task.generation";
  public static final String CURRENT_ITERATION = "current.iteration";
  public static final String RANDOM_SEED = "random.seed";


  private DatagenConnectorConfig config;
  private String topic;
  private long maxInterval;
  private int maxRecords;
  private long count = 0L;
  private org.apache.avro.Schema avroSchema;
  private int taskId;
  private Map<String, Object> sourcePartition;
  private long taskGeneration;
  private final Random random = new Random();
  private MessageGenerator messageGenerator;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    config = new DatagenConnectorConfig(props);
    topic = config.getKafkaTopic();
    maxInterval = config.getMaxInterval();
    maxRecords = config.getIterations();
    taskGeneration = 0;
    taskId = Integer.parseInt(props.get(TASK_ID));
    sourcePartition = Collections.singletonMap(TASK_ID, taskId);

    if (config.getRandomSeed() != null) {
      random.setSeed(config.getRandomSeed());
      // Each task will now deterministically advance it's random source
      // This makes it such that each task will generate different data
      for (int i = 0; i < taskId; i++) {
        random.setSeed(random.nextLong());
      }
    }

    Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
    if (offset != null) {
      //  The offset as it is stored contains our next state, so restore it as-is.
      taskGeneration = ((Long) offset.get(TASK_GENERATION)).intValue();
      count = ((Long) offset.get(CURRENT_ITERATION));
      random.setSeed((Long) offset.get(RANDOM_SEED));
    }

    

    if (config.getGeneratorType().equals("avro")) {
      log.info("Using Avro generator");
      messageGenerator = new AvroGenerator(random, count, config);
    } else if (config.getGeneratorType().equals("jsonschema")) {
      log.info("Using JSON schema generator");
      messageGenerator = new JsonSchemaGenerator(config);
    } else {
      throw new ConnectException("Invalid generator type: " + config.getGeneratorType());
    }
    
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    if (maxInterval > 0) {
      try {
        Thread.sleep((long) (maxInterval * Math.random()));
      } catch (InterruptedException e) {
        Thread.interrupted();
        return null;
      }
    }

    Message message = messageGenerator.generate();

    if (maxRecords > 0 && count >= maxRecords) {
      throw new ConnectException(
          String.format("Stopping connector: generated the configured %d number of messages", count)
      );
    }

    // Re-seed the random each time so that we can save the seed to the source offsets.
    long seed = random.nextLong();
    random.setSeed(seed);

    // The source offsets will be the values that the next task lifetime will restore from
    // Essentially, the "next" state of the connector after this loop completes
    Map<String, Object> sourceOffset = new HashMap<>();
    // The next lifetime will be a member of the next generation.
    sourceOffset.put(TASK_GENERATION, taskGeneration + 1);
    // We will have produced this record
    sourceOffset.put(CURRENT_ITERATION, count + 1);
    // This is the seed that we just re-seeded for our own next iteration.
    sourceOffset.put(RANDOM_SEED, seed);

    final ConnectHeaders headers = new ConnectHeaders();
    headers.addLong(TASK_GENERATION, taskGeneration);
    headers.addLong(TASK_ID, taskId);
    headers.addLong(CURRENT_ITERATION, count);

    final List<SourceRecord> records = new ArrayList<>();
    SourceRecord record = new SourceRecord(
        sourcePartition,
        sourceOffset,
        topic,
        null,
        message.getKey().schema(),
        message.getKey().value(),
        message.getValue().schema(),
        message.getValue().value(),
        null,
        headers
    );

    records.add(record);
    count += records.size();
    return records;
  }

  @Override
  public void stop() {
  }
}
