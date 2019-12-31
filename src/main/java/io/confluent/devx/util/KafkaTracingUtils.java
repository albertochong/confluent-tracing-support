package io.confluent.devx.util;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.ClientSpanNameProvider;
import io.opentracing.contrib.kafka.CustomHeadersMapExtractAdapter;
import io.opentracing.contrib.kafka.CustomHeadersMapInjectAdapter;
import io.opentracing.contrib.kafka.CustomSpanDecorator;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

public class KafkaTracingUtils {

  private static final Logger logger = LoggerFactory.getLogger(KafkaTracingUtils.class);

  public static final String CONFLUENT_KSQL_PREFIX = "_confluent-ksql-";
  public static final String ALLOW_KSQL_INTERNAL_TOPICS = "ALLOW_KSQL_INTERNAL_TOPICS";

  public static final String INTERCEPTORS_CONFIG_FILE = "INTERCEPTORS_CONFIG_FILE";
  public static final String TO_PREFIX = "To_";
  public static final String FROM_PREFIX = "From_";

  public static boolean isInternalTopic(String topic) {
    return topic.indexOf(CONFLUENT_KSQL_PREFIX) >= 0;
  }

  public static <K,V> Span buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer) {
    return buildAndInjectSpan(record, tracer, ClientSpanNameProvider.PRODUCER_OPERATION_NAME);
  }

  public static <K,V> Span buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer,
                                        BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {

    String producerOper = TO_PREFIX + record.topic();
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(producerSpanNameProvider.apply(producerOper, record))
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

    SpanContext spanContext = extract(record.headers(), tracer);

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Span span = spanBuilder.start();
    CustomSpanDecorator.onSend(record, span);

    try {
      inject(span.context(), record.headers(), tracer);
    } catch (Exception ex) {
      logger.error("failed to inject span context. sending record second time?", ex);
    }

    return span;

  }

  public static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer) {
    buildAndFinishChildSpan(record, tracer, ClientSpanNameProvider.CONSUMER_OPERATION_NAME);
  }

  public static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer,
                                            BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {

    SpanContext parentContext = extract(record.headers(), tracer);

    if (parentContext != null) {

      String consumerOper = FROM_PREFIX + record.topic();
      Tracer.SpanBuilder spanBuilder = tracer.buildSpan(consumerSpanNameProvider
        .apply(consumerOper, record))
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

      //spanBuilder.asChildOf(parentContext);
      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

      Span span = spanBuilder.start();
      CustomSpanDecorator.onResponse(record, span);
      span.finish();

      injectSecond(span.context(), record.headers(), tracer);

    }

  }

  private static SpanContext extract(Headers headers, Tracer tracer) {
    return tracer.extract(Format.Builtin.TEXT_MAP,
      new CustomHeadersMapExtractAdapter(headers, false));
  }

  private static void inject(SpanContext spanContext, Headers headers, Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
      new CustomHeadersMapInjectAdapter(headers, false));
  }

  private static void injectSecond(SpanContext spanContext, Headers headers, Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new CustomHeadersMapInjectAdapter(headers, true));
  }

}
