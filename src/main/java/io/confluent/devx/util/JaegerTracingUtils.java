package io.confluent.devx.util;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.opentracing.References;
import io.opentracing.Scope;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JaegerTracingUtils {

  private static final Logger logger = LoggerFactory.getLogger(JaegerTracingUtils.class);

  public static final String CONFIG_FILE_PROP = "jaeger.tracing.interceptors.config.file";
  public static final String TO_PREFIX = "To_";
  public static final String FROM_PREFIX = "From_";

  static SpanContext extract(Headers headers, Tracer tracer) {

    return tracer.extract(Format.Builtin.TEXT_MAP,
      new CustomHeadersMapExtractAdapter(headers, false));

  }

  static void inject(SpanContext spanContext, Headers headers, Tracer tracer) {

    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
      new CustomHeadersMapInjectAdapter(headers, false));

  }

  static void injectSecond(SpanContext spanContext, Headers headers, Tracer tracer) {

    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new CustomHeadersMapInjectAdapter(headers, true));

  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer) {

    return buildAndInjectSpan(record, tracer, ClientSpanNameProvider.PRODUCER_OPERATION_NAME);

  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer,
                                        BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {

    String producerOper = TO_PREFIX + record.topic();
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(producerSpanNameProvider.apply(producerOper, record))
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

    SpanContext spanContext = JaegerTracingUtils.extract(record.headers(), tracer);

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Scope scope = spanBuilder.startActive(false);
    CustomSpanDecorator.onSend(record, scope.span());

    try {
      JaegerTracingUtils.inject(scope.span().context(), record.headers(), tracer);
    } catch (Exception e) {
      logger.error("failed to inject span context. sending record second time?", e);
    }

    return scope;

  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer) {
    buildAndFinishChildSpan(record, tracer, ClientSpanNameProvider.CONSUMER_OPERATION_NAME);
  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer,
                                            BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {

    SpanContext parentContext = JaegerTracingUtils.extract(record.headers(), tracer);

    if (parentContext != null) {

      String consumerOper = FROM_PREFIX + record.topic();
      Tracer.SpanBuilder spanBuilder = tracer.buildSpan(consumerSpanNameProvider.apply(consumerOper, record))
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

      spanBuilder.asChildOf(parentContext);
      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

      Span span = spanBuilder.start();
      CustomSpanDecorator.onResponse(record, span);
      span.finish();

      JaegerTracingUtils.injectSecond(span.context(), record.headers(), tracer);

    }

  }

  /**
   * Builds a mapping between topics and tracers, so the interceptors knows
   * which tracer to use given a topic. This mapping is important because it
   * provides a way to cache tracers and we can avoid the time spent during
   * tracer instantiation. Also, it is important to provide a rapid way to
   * retrieve a tracer given the topic name, preferably using a O(1) retrieval
   * method such as a Hashtable.
   * 
   */
  public static Map<String, Tracer> buildTracerMapping(String configFileName)
    throws FileNotFoundException, IOException {

    Map<String, Tracer> mapping = null;
    File file = new File(configFileName);

    if (file.exists()) {

      mapping = new HashMap<String, Tracer>();
      List<Service> services = loadServices(file);

      for (Service service : services) {

        Tracer tracer = service.getConfig().getTracer();

        for (String topic : service.getTopics()) {
          mapping.put(topic, tracer);
        }

      }

    } else {

      throw new FileNotFoundException("The file '" + configFileName + "' does not exist.");

    }

    return mapping;

  }

  /**
   * Load the services from the interceptors configuration file. This file
   * has an single attribute called 'services' of type array, and this array
   * has AT LEAST ONE or multiple services within. Each service MUST have
   * a name and OPTIONAL attributes for 'config' and 'topics'.
   * 
   * Here is an example of two services defines, each one with their own
   * tracer configuration and topics definition.
   * 
   * {
   * 
   *   "services" : [
   * 
   *      {
   
             "service" : "Service-1",
             "config" : {
                "sampler" : {
                  "type" : "const",
                  "param" : 1
                },
                "reporter" : {
                  "logSpans" : true,
                  "flushIntervalMs" : 1000,
                  "maxQueueSize" : 100
                }
             },
             "topics" : ["Topic-1", "Topic-2", Topic-3]
   * 
   *      },
   *      {
   
             "service" : "Service-2",
             "config" : {
                "reporter" : {
                  "logSpans" : false,
                  "maxQueueSize" : 100
                }
             },
             "topics" : ["Topic-4", "Topic-5", Topic-6]
   * 
   *      }
   * 
   *   ]
   * 
   * }
   * 
   * It is important to note that the topics defined in this file
   * will be used as keys to retrieve the correspondent tracer,
   * which in turn is created per service. Therefore, a topic
   * should not belong to two different services at the same
   * time, otherwise there will be collapses and undesirable
   * tracing behavior.
   * 
   */
  private static List<Service> loadServices(File file)
    throws FileNotFoundException, IOException {

    List<Service> services = new ArrayList<Service>();

    try (FileReader reader = new FileReader(file)) {

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(reader);
      JsonObject root = element.getAsJsonObject();
      JsonArray svcs = root.getAsJsonArray("services");

      for (int i = 0; i < svcs.size(); i++) {

        JsonObject svc = svcs.get(i).getAsJsonObject();

        String serviceName = svc.get("service").getAsString();
        JsonObject configJson = svc.getAsJsonObject("config");
        Configuration config = getConfig(serviceName, configJson);
        JsonArray topicsArray = svc.getAsJsonArray("topics");
        List<String> topics = getTopics(topicsArray);

        services.add(new Service(serviceName, config, topics));

      }

    }

    return services;

  }

  private static Configuration getConfig(String serviceName, JsonObject configJson) {

    if (configJson == null) {

      // If nothing was provided, then create a default one...

      SamplerConfiguration samplerConfig =
        SamplerConfiguration.fromEnv()
        .withType("const").withParam(1);

      ReporterConfiguration reporterConfig =
        ReporterConfiguration.fromEnv()
        .withLogSpans(true);

      Configuration config =
        new Configuration(serviceName)
        .withSampler(samplerConfig)
        .withReporter(reporterConfig);

      return config;

    }

    SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();
    JsonObject sampler = configJson.getAsJsonObject("sampler");

    if (sampler != null) {

      JsonElement typeEle = sampler.get("type");
      samplerConfig.withType(typeEle != null ? typeEle.getAsString() : "const");

      JsonElement paramEle = sampler.get("param");
      samplerConfig.withParam(paramEle != null ? paramEle.getAsDouble() : 1);

    } else {

      samplerConfig.withType("const").withParam(1);

    }

    ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv();
    JsonObject reporter = configJson.getAsJsonObject("reporter");

    if (reporter != null) {

      JsonElement logSpansEle = reporter.get("logSpans");
      reporterConfig.withLogSpans(logSpansEle != null ? logSpansEle.getAsBoolean() : true);

      JsonElement flushIntervalMsEle = reporter.get("flushIntervalMs");
      reporterConfig.withFlushInterval(flushIntervalMsEle != null ?
        flushIntervalMsEle.getAsInt() : RemoteReporter.DEFAULT_FLUSH_INTERVAL_MS);

      JsonElement maxQueueSizeEle = reporter.get("maxQueueSize");
      reporterConfig.withMaxQueueSize(maxQueueSizeEle != null ?
        maxQueueSizeEle.getAsInt() : RemoteReporter.DEFAULT_MAX_QUEUE_SIZE);

    }

    return new Configuration(serviceName)
      .withSampler(samplerConfig)
      .withReporter(reporterConfig);

  }

  private static List<String> getTopics(JsonArray topicsArray) {

    List<String> topics = new ArrayList<String>();

    if (topicsArray != null) {

      for (int i = 0; i < topicsArray.size(); i++) {
        topics.add(topicsArray.get(i).getAsString());
      }

    }

    return topics;

  }

  private static class Service {

    private String serviceName;
    private Configuration config;
    private List<String> topics;

    public Service(String serviceName, Configuration config,
      List<String> topics) {

        this.serviceName = serviceName;
        this.config = config;
        this.topics = topics;

    }

    public String getServiceName() {

      return this.serviceName;

    }

    public Configuration getConfig() {

      return this.config;

    }

    public List<String> getTopics() {

      return this.topics;

    }

  }

}