package io.confluent.devx.util;

import io.opentracing.Tracer;
import io.opentracing.contrib.tracerresolver.TracerResolver;
import io.opentracing.util.GlobalTracer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class JaegerTracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  private Map<String, Tracer> tracerMapping;

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    Tracer tracer = getTracer(record.topic());
    System.out.println("-------------------------> " + tracer);

    JaegerTracingUtils.buildAndInjectSpan(record, tracer).finish();
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

    String interceptorsConfigFile = System.getenv(JaegerTracingUtils.INTERCEPTORS_CONFIG_FILE);

    if (interceptorsConfigFile != null) {

      try {
        tracerMapping = JaegerTracingUtils.buildTracerMapping(interceptorsConfigFile);
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