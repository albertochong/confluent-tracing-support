package io.opentracing.contrib.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.opentracing.Span;

public class CustomSpanDecorator extends SpanDecorator {

    public static <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
        SpanDecorator.onSend(record, span);
    }

    public static <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
        SpanDecorator.onResponse(record, span);
    }

}