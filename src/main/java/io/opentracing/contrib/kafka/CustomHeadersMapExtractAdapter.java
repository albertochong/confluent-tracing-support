package io.opentracing.contrib.kafka;

import org.apache.kafka.common.header.Headers;

public class CustomHeadersMapExtractAdapter extends HeadersMapExtractAdapter {

  public CustomHeadersMapExtractAdapter(Headers headers, boolean second) {
    super(headers);
  }

}