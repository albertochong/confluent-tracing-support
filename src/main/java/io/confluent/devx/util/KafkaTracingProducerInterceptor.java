package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaTracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  private String ksqlServiceId;
  private boolean allowKsqlInternalTopics;
  private Map<String, Tracer> tracerMapping;

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    String topic = record.topic();
    Tracer tracer = getTracer(topic);

    if (KafkaTracingUtils.isInternalTopic(topic, ksqlServiceId)) {

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
    ksqlServiceId = (String) configs.get(KafkaTracingUtils.KSQL_SERVICE_ID_PARAM);

    if (ksqlServiceId == null) {
      ksqlServiceId = KafkaTracingUtils.KSQL_SERVICE_ID_DEFAULT;
    }

    String interceptorsConfigFile = System.getenv(KafkaTracingUtils.INTERCEPTORS_CONFIG_FILE);

    if (interceptorsConfigFile != null) {

      try {
        tracerMapping = KafkaTracingUtils.buildTracerMapping(interceptorsConfigFile);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
 
    } else {

      Tracer tracer = TracerResolver.resolveTracer();
      System.out.println("-------------> " + tracer);
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