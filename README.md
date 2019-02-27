# Jaeger Tracing Support for REST Proxy, Connect and KSQL
Interceptors-based approach to implement distributed tracing using Jaeger

## Requirements

- Java 8+
- Kafka 2.0.0+

## Overview

The interceptors from the [OpenTracing Apache Kafka Client project](https://github.com/opentracing-contrib/java-kafka-client) are great implementations to use with Java-based applications where you own the code and are able to instantiate your own tracers and make them available throughout the JVM. However; there are situations in which records will be produced and consumed from JVMs that are automatically created for you, and you don't have any way to instantiate your own tracers because its source-code is not available. Or maybe it is... but you are not allowed to change it.

Typical examples include the use of [REST Proxy](https://docs.confluent.io/current/kafka-rest/docs/index.html), [Kafka Connect](https://docs.confluent.io/current/connect/index.html), and [Confluent's KSQL Servers](https://docs.confluent.io/current/ksql/docs/index.html). In this technologies, the JVMs are automatically created by pre-defined scripts and you would need a special type of interceptor, one that can instantiate their own tracers based on configuration specified in an external file. For this particular situation, you can use the Jaeger Tracing support provided by this project.

![Sample](images/sample.png)

The example above shows the tracing across different Kafka topics. The first topic called `test` received a record sent by REST Proxy, which in turn had the interceptors installed. Then, the record is consumed by a stream called `FIRST_STREAM` created on KSQL, which also had the interceptors installed. Still in KSQL; multiple other streams (`SECOND_STREAM`, `THIRD_STREAM`, etc) were created to copy data from/to another stream, to test if the tracing can keep up with the flow.

## Usage

For example, if you want to use these interceptors with a KSQL Server, you need to edit the properties configuration file used to start the server and include the following lines:

```java
bootstrap.servers=localhost:9092
listeners=http://localhost:8088
auto.offset.reset=earliest

############################## Jaeger Tracing Configuration ################################

producer.interceptor.classes=io.confluent.devx.util.JaegerTracingProducerInterceptor
consumer.interceptor.classes=io.confluent.devx.util.JaegerTracingConsumerInterceptor

jaeger.tracing.interceptors.config.file=interceptorsConfig.json

############################################################################################
```
Note that we provided a JSON configuration file via the property `jaeger.tracing.interceptors.config.file`. This file contains the definition of all services that need to be traced, as well as the configuration of the tracer for each service. This is important because the JVM could be used to host multiple services at a time, each one belonging to a different domain. Since each service will probably use different Kafka topics to implement their processing logic, this file helps in keeping a mapping between services and topics so the tracing is executed separately for each service.

Here is an example:

```json
{
   "services":[
      {
         "service":"CustomerService",
         "config":{
            "sampler":{
               "type":"const",
               "param":1
            },
            "reporter":{
               "logSpans":true,
               "flushIntervalMs":1000,
               "maxQueueSize":10
            }
         },
         "topics":[
            "Topic-1",
            "Topic-2",
            "Topic-3"
         ]
      },
      {
         "service":"ProductService",
         "config":{
            "reporter":{
               "logSpans":false,
               "maxQueueSize":100
            }
         },
         "topics":[
            "Topic-4",
            "Topic-5"
         ]
      }
   ]
}
```
In the example above, two services were defined, `CustomerService` and `ProductService` respectively. In runtime, it will be created one tracer for each one of these services. However, every time a record is either produced or consumed for topic `Topic-1`, the interceptor knows that it should use the tracer associated for `CustomerService`, as well as every time a record is either produced or consumed for topic `Topic-4`, the interceptor knows that it should use the tracer associated for `ProductService`.

These are the dependencies that you will need to install in your classpath along with the interceptors:

```xml
<dependency>
    <groupId>io.opentracing</groupId>
    <artifactId>opentracing-api</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.opentracing</groupId>
    <artifactId>opentracing-util</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-client</artifactId>
    <version>VERSION</version>
</dependency>    

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-client</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-core</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.opentracing</groupId>
    <artifactId>opentracing-noop</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-thrift</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>org.apache.thrift</groupId>
    <artifactId>libthrift</artifactId>
    <version>VERSION</version>
</dependency>
```

## License

[Apache 2.0 License](./LICENSE).