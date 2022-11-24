/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;


import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public class KafkaConsumerConfig {
    private static final long DEFAULT_MESSAGES_COUNT = 10;

    private final String topic;
    private final String autoOffsetReset = "earliest"; // ??
    private final String enableAutoCommit = "false"; // ??
    private final Long messageCount;
    private final TracingSystem tracingSystem;
    private final String additionalConfig;

    private static final Map<String, String> DEFAULT_PROPERTIES = Map.ofEntries(
        entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
        entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    );

    private static final Map<String, String> DEFAULT_TRUSTSTORE_CONFIGS = Map.ofEntries(
        entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL"),
        entry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM"));

    private static final Map<String, String> DEFAULT_KEYSTORE_CONFIGS = Map.ofEntries(
        entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL"),
        entry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM"));

    public KafkaConsumerConfig(String topic, Long messageCount, TracingSystem tracingSystem, String additionalConfig) {
        this.topic = topic;
        this.messageCount = messageCount;
        this.tracingSystem = tracingSystem;
        this.additionalConfig = additionalConfig;
    }

    public static KafkaConsumerConfig fromEnv() {
        String topic = System.getenv("TOPIC");
        Long messageCount = System.getenv("MESSAGE_COUNT") == null ? DEFAULT_MESSAGES_COUNT : Long.parseLong(System.getenv("MESSAGE_COUNT"));
        TracingSystem tracingSystem = TracingSystem.forValue(System.getenv().getOrDefault("TRACING_SYSTEM", ""));
        String additionalConfig = System.getenv().getOrDefault("ADDITIONAL_CONFIG", "");

        return new KafkaConsumerConfig(topic, messageCount, tracingSystem, additionalConfig);
    }

    public static String convertEnvVarToPropertyKey(String envVar) {
        System.out.println("ENV_VAR " + envVar);
        return envVar.substring(6).toLowerCase().replace("_", ".");
    }

    public static Properties createProperties(KafkaConsumerConfig config) {
        Properties props = new Properties();

        Map<String, String> userConfigs = System.getenv()
                .entrySet()
                .stream()
                .filter(map -> map.getKey().startsWith("KAFKA_"))
                .collect(Collectors.toMap(map -> convertEnvVarToPropertyKey(map.getKey()), map -> map.getValue()));

        // Set default configurations
        props.putAll(DEFAULT_PROPERTIES);

        if ( userConfigs.containsKey(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG) ) {
            props.putAll(DEFAULT_TRUSTSTORE_CONFIGS);
        }

        if ( userConfigs.containsKey(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)
          && userConfigs.containsKey(SslConfigs.SSL_KEYSTORE_KEY_CONFIG) ) {
            props.putAll(DEFAULT_KEYSTORE_CONFIGS);
        }

        // Set user provided configurations overriding any overlapping default configurations
        props.putAll(userConfigs);

        if (!config.getAdditionalConfig().isEmpty()) {
            StringTokenizer tok = new StringTokenizer(config.getAdditionalConfig(), System.lineSeparator());
            while (tok.hasMoreTokens()) {
                String record = tok.nextToken();
                int endIndex = record.indexOf('=');
                if (endIndex == -1) {
                    throw new RuntimeException("Failed to parse Map from String");
                }
                String key = record.substring(0, endIndex);
                String value = record.substring(endIndex + 1);
                props.put(key.trim(), value.trim());

            }
        }
        /*
        if ((config.getOauthAccessToken() != null)
                || (config.getOauthTokenEndpointUri() != null && config.getOauthClientId() != null && config.getOauthRefreshToken() != null)
                || (config.getOauthTokenEndpointUri() != null && config.getOauthClientId() != null && config.getOauthClientSecret() != null))    {
            log.info("Configuring OAuth");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL".equals(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) ? "SASL_SSL" : "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            if (!(additionalProps.containsKey(SaslConfigs.SASL_MECHANISM) && additionalProps.getProperty(SaslConfigs.SASL_MECHANISM).equals("PLAIN"))) {
                props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, config.saslLoginCallbackClass);
            }
        }

        props.putAll(additionalProps);*/
        return props;
    }


    public String getTopic() {
        return topic;
    }

    public String getAutoOffsetReset() { // not used in KafkaConsumerExample
        return autoOffsetReset;
    }

    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public Long getMessageCount() {
        return messageCount;
    }

    public TracingSystem getTracingSystem() {
        return tracingSystem;
    }

    public String getAdditionalConfig() {
        return additionalConfig;
    }


    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
                "topic='" + topic + '\'' +
                ", autoOffsetReset='" + autoOffsetReset + '\'' +
                ", enableAutoCommit='" + enableAutoCommit + '\'' +
                ", messageCount=" + messageCount +
                ", tracingSystem='" + tracingSystem + '\'' +
                ", additionalConfig='" + additionalConfig + '\'' +
                kafkaFieldsToString() +
                '}';
    }

    public static String kafkaFieldsToString() {
        StringBuilder sb = new StringBuilder();
        Map<String, String> envVars = System.getenv();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            if (entry.getKey().contains("KAFKA")) {
                String key = convertEnvVarToPropertyKey(entry.getKey());
                String value = entry.getValue();
                sb.append(", " + key + "='" + value + "\'");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        KafkaConsumerConfig KCC = KafkaConsumerConfig.fromEnv();
        Properties props = createProperties(KCC);
        System.out.println("\n" + KCC.toString()); // prints the properties that have been passed to toString()
        System.out.println("These are the props: " + props);
    }
}