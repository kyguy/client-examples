/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.streams.TracingKafkaClientSupplier;
import io.opentracing.util.GlobalTracer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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

        log.info(KafkaStreamsConfig.class.getName() + ": {}", config.toString());

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
        if (tracingSystem != null) {

            if (tracingSystem == TracingSystem.JAEGER) {
                Tracer tracer = Configuration.fromEnv().getTracer();
                GlobalTracer.registerIfAbsent(tracer);

                KafkaClientSupplier supplier = new TracingKafkaClientSupplier(tracer);
                streams = new KafkaStreams(builder.build(), props, supplier);
            } else if (tracingSystem == TracingSystem.OPENTELEMETRY) {

                props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, io.opentelemetry.instrumentation.kafkaclients.TracingProducerInterceptor.class.getName());
                props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, io.opentelemetry.instrumentation.kafkaclients.TracingConsumerInterceptor.class.getName());
                streams = new KafkaStreams(builder.build(), props);
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