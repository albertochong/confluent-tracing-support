package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaTracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  private boolean allowKsqlInternalTopics;

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    String topic = record.topic();
    Tracer tracer = GlobalTracer.get();

    if (KafkaTracingUtils.isInternalTopic(topic)) {
      if (allowKsqlInternalTopics) {
        KafkaTracingUtils.buildAndInjectSpan(record, tracer).finish();
      }
    } else {
      KafkaTracingUtils.buildAndInjectSpan(record, tracer).finish();
    }

    return record;

  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
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