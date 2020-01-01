package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingProducerInterceptor;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ConfluentProducerInterceptor<K, V> extends TracingProducerInterceptor<K, V> {

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    if (Utils.isInternalTopic(record.topic())) {
      if (Utils.allowKsqlInternalTopics()) {
        super.onSend(record);
      }
    } else {
      super.onSend(record);
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
    Utils.readAllowKsqlInternalTopics();
    Tracer tracer = TracerResolver.resolveTracer();
    GlobalTracer.registerIfAbsent(tracer);
  }

}