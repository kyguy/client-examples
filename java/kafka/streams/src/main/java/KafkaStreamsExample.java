/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import strimzi.io.TracingSystem;

import java.util.Properties;

public class KafkaStreamsExample {
    private static final Logger log = LogManager.getLogger(KafkaStreamsExample.class);

    public static void main(String[] args) {
        KafkaStreamsConfig config = KafkaStreamsConfig.fromEnv();

        log.info(KafkaStreamsConfig.class.getName() + ": {}",  config.toString());

        Properties props = KafkaStreamsConfig.createProperties(config);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(config.getSourceTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(value);
                    return sb.reverse().toString();
                })
                .to(config.getTargetTopic(), Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams;

        TracingSystem tracingSystem = config.getTracingSystem();

        log.info(" ======= DEV VERSION {} ==========", 1.0 );

        if (tracingSystem != null) {

            if (tracingSystem == TracingSystem.JAEGER) {
                Tracer tracer = Configuration.fromEnv().getTracer();
                GlobalTracer.registerIfAbsent(tracer);

                KafkaClientSupplier supplier = new  io.opentracing.contrib.kafka.streams.TracingKafkaClientSupplier(tracer);
                streams = new KafkaStreams(builder.build(), props, supplier);
            } else if (tracingSystem == TracingSystem.OPENTELEMETRY) {

                //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
                //props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.applicationId);
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                //props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor.class.getName());
                //props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor.class.getName());

                KafkaClientSupplier supplier = new TracingKafkaClientSupplier();
                streams = new KafkaStreams(builder.build(), props, supplier);

                log.info("OPENTELEMETRY TRACING USED + CONFIGURED, SERDES {} DESERIALIZER {} SERIALIZER {}", Serdes.String().getClass(), StringDeserializer.class.getName(), StringSerializer.class.getName());
            } else {
                log.error("Error: TRACING_SYSTEM {} is not recognized or supported!", config.getTracingSystem());
                streams = new KafkaStreams(builder.build(), props);
            }
        } else {
            streams = new KafkaStreams(builder.build(), props);
        }

        streams.start();
    }
}