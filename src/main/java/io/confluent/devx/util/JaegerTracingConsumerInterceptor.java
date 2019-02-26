package io.confluent.devx.util;

import io.opentracing.Tracer;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class JaegerTracingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  private Map<String, Tracer> tracerMapping;

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {

    if (tracerMapping != null) {

      Tracer tracer = null;

      for (ConsumerRecord<K, V> record : records) {
  
        tracer = tracerMapping.get(record.topic());
  
        if (tracer != null) {
          JaegerTracingUtils.buildAndFinishChildSpan(record, tracer);
        }
  
      }

    }

    return records;

  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

    if (configs.containsKey(JaegerTracingUtils.CONFIG_FILE_PROP)) {

      String configFileName = (String) configs.get(JaegerTracingUtils.CONFIG_FILE_PROP);

      try {
        tracerMapping = JaegerTracingUtils.buildTracerMapping(configFileName);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
 
    }

  }

}