package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingConsumerInterceptor;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ConfluentConsumerInterceptor<K, V> extends TracingConsumerInterceptor<K, V> {

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {

    for (ConsumerRecord<K, V> record : records) {
      if (Utils.isInternalTopic(record.topic())) {
        if (Utils.allowKsqlInternalTopics()) {
          TracingKafkaUtils.buildAndFinishChildSpan(record, GlobalTracer.get());
        }
      } else {
        TracingKafkaUtils.buildAndFinishChildSpan(record, GlobalTracer.get());
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
    Utils.readAllowKsqlInternalTopics();
    Tracer tracer = TracerResolver.resolveTracer();
    GlobalTracer.registerIfAbsent(tracer);
  }

}