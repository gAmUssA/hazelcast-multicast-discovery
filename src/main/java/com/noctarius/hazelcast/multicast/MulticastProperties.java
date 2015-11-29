package com.noctarius.hazelcast.multicast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;

import static com.hazelcast.config.properties.PropertyTypeConverter.BOOLEAN;
import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

public class MulticastProperties {

    public static final PropertyDefinition MULTICAST_GROUP = property("multicast-group", STRING);
    public static final PropertyDefinition MULTICAST_PORT = property("multicast-port", INTEGER, new PortValueValidator());
    public static final PropertyDefinition MULTICAST_TIMEOUT = property("multicast-timeout", INTEGER);
    public static final PropertyDefinition MULTICAST_TIME_TO_LIVE = property("multicast-time-to-live", INTEGER);
    public static final PropertyDefinition TRUSTED_INTERFACES = property("trusted-interfaces", STRING);
    public static final PropertyDefinition LOOPBACK_MODE_ENABLED = property("loopback-mode-enabled", BOOLEAN);
    public static final PropertyDefinition SERVICE_NAME = property("service-name", STRING);

    public static final String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    public static final int DEFAULT_MULTICAST_PORT = 54327;
    public static final int DEFAULT_MULTICAST_TIMEOUT = 2;
    public static final int DEFAULT_MULTICAST_TIME_TO_LIVE = 32;
    public static final boolean DEFAULT_LOOPBACK_MODE_ENABLED = true;
    public static final String DEFAULT_SERVICE_NAME = "hz::Member";

    public static final String ENV_PREFIX = "com.hazelcast.discovery";

    // Prevent instantiation
    private MulticastProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter, ValueValidator valueValidator) {

        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }

    private static class PortValueValidator
            implements ValueValidator<Integer> {

        public void validate(Integer value)
                throws ValidationException {
            if (value < 0) {
                throw new ValidationException("proxy-port number must be greater 0");
            }
            if (value > 65535) {
                throw new ValidationException("proxy-port number must be less or equal to 65535");
            }
        }
    }
}
