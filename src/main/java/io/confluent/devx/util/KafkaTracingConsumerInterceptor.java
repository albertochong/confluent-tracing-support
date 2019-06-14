package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaTracingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  private Map<String, Tracer> tracerMapping;

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {

    for (ConsumerRecord<K, V> record : records) {

      Tracer tracer = getTracer(record.topic());
      System.out.println("-------------------------> " + tracer);
      KafkaTracingUtils.buildAndFinishChildSpan(record, tracer);

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

    String interceptorsConfigFile = System.getenv(KafkaTracingUtils.INTERCEPTORS_CONFIG_FILE);

    if (interceptorsConfigFile != null) {

      try {
        tracerMapping = KafkaTracingUtils.buildTracerMapping(interceptorsConfigFile);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
 
    } else {

      Tracer tracer = TracerResolver.resolveTracer();
      GlobalTracer.registerIfAbsent(tracer);

    }

  }

  private Tracer getTracer(String topic) {

    Tracer tracer = GlobalTracer.get();

    if (tracerMapping != null) {

      if (tracerMapping.containsKey(topic)) {
        tracer = tracerMapping.get(topic);
      }

    }

    return tracer;

  }

}