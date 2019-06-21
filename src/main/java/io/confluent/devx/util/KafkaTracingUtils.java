package io.confluent.devx.util;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.reporters.RemoteReporter;
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

public class KafkaTracingUtils {

  private static final Logger logger = LoggerFactory.getLogger(KafkaTracingUtils.class);

  public static final String KSQL_SERVICE_ID_PARAM = "ksql.service.id";
  public static final String KSQL_SERVICE_ID_DEFAULT = "default_";
  public static final String CONFLUENT_KSQL_PREFIX = "_confluent-ksql-";
  public static final String ALLOW_KSQL_INTERNAL_TOPICS = "ALLOW_KSQL_INTERNAL_TOPICS";

  public static final String INTERCEPTORS_CONFIG_FILE = "INTERCEPTORS_CONFIG_FILE";
  public static final String TO_PREFIX = "To_";
  public static final String FROM_PREFIX = "From_";

  public static boolean isInternalTopic(String topic, String ksqlServiceId) {
    return topic.indexOf(CONFLUENT_KSQL_PREFIX + ksqlServiceId) >= 0;
  }

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

    }

    return mapping;

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

        services.add(new Service(config, topics));

      }

    }

    return services;

  }

  private static Configuration getConfig(String serviceName, JsonObject configJson) {

    if (configJson == null) {

      return new Configuration(serviceName)
        .withSampler(SamplerConfiguration.fromEnv())
        .withReporter(ReporterConfiguration.fromEnv());

    }

    SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();
    JsonObject sampler = configJson.getAsJsonObject("sampler");

    if (sampler != null) {

      JsonElement typeEle = sampler.get("type");
      samplerConfig.withType(typeEle != null ? typeEle.getAsString() : "const");

      JsonElement paramEle = sampler.get("param");
      samplerConfig.withParam(paramEle != null ? paramEle.getAsDouble() : 1);

      JsonElement mgmrHostPortEle = sampler.get("managerHostPort");
      samplerConfig.withManagerHostPort(mgmrHostPortEle != null ?
        mgmrHostPortEle.getAsString() : null);

    }

    ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv();
    JsonObject reporter = configJson.getAsJsonObject("reporter");

    if (reporter != null) {

      JsonElement logSpansEle = reporter.get("logSpans");
      reporterConfig.withLogSpans(logSpansEle != null ?
        logSpansEle.getAsBoolean() : true);

      JsonElement flushIntervalMsEle = reporter.get("flushIntervalMs");
      reporterConfig.withFlushInterval(flushIntervalMsEle != null ?
        flushIntervalMsEle.getAsInt() : RemoteReporter.DEFAULT_FLUSH_INTERVAL_MS);

      JsonElement maxQueueSizeEle = reporter.get("maxQueueSize");
      reporterConfig.withMaxQueueSize(maxQueueSizeEle != null ?
        maxQueueSizeEle.getAsInt() : RemoteReporter.DEFAULT_MAX_QUEUE_SIZE);

      JsonObject sender = reporter.getAsJsonObject("sender");

      if (sender != null) {

        JsonElement agentHostEle = reporter.get("agentHost");
        reporterConfig.getSenderConfiguration()
          .withAgentHost(agentHostEle != null ?
          agentHostEle.getAsString() : null);

        JsonElement agentPortEle = reporter.get("agentPort");
        reporterConfig.getSenderConfiguration()
          .withAgentPort(agentPortEle != null ?
          agentPortEle.getAsInt() : 0);

        JsonElement endpointEle = reporter.get("endpoint");
        reporterConfig.getSenderConfiguration()
          .withEndpoint(endpointEle != null ?
          endpointEle.getAsString() : null);

        JsonElement authTokenEle = reporter.get("authToken");
        reporterConfig.getSenderConfiguration()
          .withAuthToken(authTokenEle != null ?
          authTokenEle.getAsString() : null);

        JsonElement authUsernameEle = reporter.get("authUsername");
        reporterConfig.getSenderConfiguration()
          .withAuthUsername(authUsernameEle != null ?
          authUsernameEle.getAsString() : null);

        JsonElement authPasswordEle = reporter.get("authPassword");
        reporterConfig.getSenderConfiguration()
          .withAuthPassword(authPasswordEle != null ?
          authPasswordEle.getAsString() : null);

      }

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

    private Configuration config;
    private List<String> topics;

    public Service(Configuration config, List<String> topics) {
        this.config = config;
        this.topics = topics;
    }

    public Configuration getConfig() {
      return this.config;
    }

    public List<String> getTopics() {
      return this.topics;
    }

  }

}