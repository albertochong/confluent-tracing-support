package io.confluent.devx.util;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Scope;
import io.opentracing.Tracer;
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

    try (Scope scope = JaegerTracingUtils.buildAndInjectSpan(record, tracer)) {
      scope.span().finish();
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
 
    } else {

      if (!GlobalTracer.isRegistered()) {

        GlobalTracer.register(new Configuration(
          JaegerTracingUtils.class.getSimpleName())
            .withSampler(SamplerConfiguration.fromEnv().withType("const").withParam(1))
            .withReporter(ReporterConfiguration.fromEnv().withLogSpans(true)).getTracer());

      }

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