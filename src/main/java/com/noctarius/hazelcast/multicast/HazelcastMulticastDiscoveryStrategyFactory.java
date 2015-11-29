package com.noctarius.hazelcast.multicast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HazelcastMulticastDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {

    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        List<PropertyDefinition> propertyDefinitions = new ArrayList<PropertyDefinition>();
        propertyDefinitions.add(MulticastProperties.LOOPBACK_MODE_ENABLED);
        propertyDefinitions.add(MulticastProperties.MULTICAST_GROUP);
        propertyDefinitions.add(MulticastProperties.MULTICAST_PORT);
        propertyDefinitions.add(MulticastProperties.MULTICAST_TIME_TO_LIVE);
        propertyDefinitions.add(MulticastProperties.MULTICAST_TIMEOUT);
        propertyDefinitions.add(MulticastProperties.TRUSTED_INTERFACES);
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(propertyDefinitions);
    }

    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return HazelcastMulticastDiscoveryStrategy.class;
    }

    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode localNode, ILogger logger, Map<String, Comparable> properties) {
        return new HazelcastMulticastDiscoveryStrategy(localNode, logger, properties);
    }

    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }
}
