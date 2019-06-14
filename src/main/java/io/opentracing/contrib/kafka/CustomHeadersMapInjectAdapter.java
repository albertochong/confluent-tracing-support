package io.opentracing.contrib.kafka;

import org.apache.kafka.common.header.Headers;

public class CustomHeadersMapInjectAdapter extends HeadersMapInjectAdapter {

  public CustomHeadersMapInjectAdapter(Headers headers, boolean second) {
    super(headers);
  }

}