package io.confluent.devx.util;

public class Utils {

  public static final String CONFLUENT_KSQL_PREFIX = "_confluent-ksql-";
  public static final String ALLOW_KSQL_INTERNAL_TOPICS = "ALLOW_KSQL_INTERNAL_TOPICS";
  private static boolean allowKsqlInternalTopics;

  public static void readAllowKsqlInternalTopics() {
    String _allowKsqlInternalTopics = System.getenv(Utils.ALLOW_KSQL_INTERNAL_TOPICS);
    allowKsqlInternalTopics = Boolean.parseBoolean(_allowKsqlInternalTopics);
  }

  public static boolean allowKsqlInternalTopics() {
    return allowKsqlInternalTopics;
  }

  public static boolean isInternalTopic(String topic) {
    return topic.indexOf(CONFLUENT_KSQL_PREFIX) >= 0;
  }

}
