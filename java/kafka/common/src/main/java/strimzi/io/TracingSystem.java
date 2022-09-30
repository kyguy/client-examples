package strimzi.io;

public enum TracingSystem {
    JAEGER,
    OPENTELEMETRY;

    public static TracingSystem forValue(String value) {
        switch (value) {
            case "jaeger":
                return TracingSystem.JAEGER;
            case "opentelemetry":
                return TracingSystem.OPENTELEMETRY;
            default:
                return null;
        }
    }
}
