package io.confluent.devx.util;

import io.opentracing.Scope;
import io.opentracing.Tracer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class JaegerTracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  private Map<String, Tracer> tracerMapping;

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    if (tracerMapping != null) {

      Tracer tracer = tracerMapping.get(record.topic());

      if (tracer != null) {
  
        try (Scope scope = JaegerTracingUtils.buildAndInjectSpan(record, tracer)) {
          scope.span().finish();
        }
  
      }

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