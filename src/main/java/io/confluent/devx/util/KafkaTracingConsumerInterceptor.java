package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaTracingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  private boolean allowKsqlInternalTopics;

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {

    for (ConsumerRecord<K, V> record : records) {

      String topic = record.topic();
      Tracer tracer = GlobalTracer.get();

      if (KafkaTracingUtils.isInternalTopic(topic)) {
        if (allowKsqlInternalTopics) {
          KafkaTracingUtils.buildAndFinishChildSpan(record, tracer);
        }
      } else {
        KafkaTracingUtils.buildAndFinishChildSpan(record, tracer);
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

    String _allowKsqlInternalTopics = System.getenv(KafkaTracingUtils.ALLOW_KSQL_INTERNAL_TOPICS);
    allowKsqlInternalTopics = Boolean.parseBoolean(_allowKsqlInternalTopics);

    Tracer tracer = TracerResolver.resolveTracer();
    System.out.println("-------------> " + tracer);
    GlobalTracer.registerIfAbsent(tracer);

  }

}