package io.confluent.devx.util;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

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

    for (ConsumerRecord<K, V> record : records) {

      Tracer tracer = getTracer(record.topic());
      JaegerTracingUtils.buildAndFinishChildSpan(record, tracer);

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