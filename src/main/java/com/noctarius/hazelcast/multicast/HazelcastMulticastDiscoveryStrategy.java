package com.noctarius.hazelcast.multicast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_LOOPBACK_MODE_ENABLED;
import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_MULTICAST_GROUP;
import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_MULTICAST_PORT;
import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_MULTICAST_TIMEOUT;
import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_MULTICAST_TIME_TO_LIVE;
import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_SERVICE_NAME;
import static com.noctarius.hazelcast.multicast.MulticastProperties.ENV_PREFIX;
import static com.noctarius.hazelcast.multicast.MulticastProperties.LOOPBACK_MODE_ENABLED;
import static com.noctarius.hazelcast.multicast.MulticastProperties.MULTICAST_GROUP;
import static com.noctarius.hazelcast.multicast.MulticastProperties.MULTICAST_PORT;
import static com.noctarius.hazelcast.multicast.MulticastProperties.MULTICAST_TIMEOUT;
import static com.noctarius.hazelcast.multicast.MulticastProperties.MULTICAST_TIME_TO_LIVE;
import static com.noctarius.hazelcast.multicast.MulticastProperties.SERVICE_NAME;
import static com.noctarius.hazelcast.multicast.MulticastProperties.TRUSTED_INTERFACES;

public class HazelcastMulticastDiscoveryStrategy
        extends AbstractDiscoveryStrategy {

    private final DiscoveryService discoveryService;

    public HazelcastMulticastDiscoveryStrategy(DiscoveryNode localNode, ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        this.discoveryService = createMulticaster(localNode);
    }

    public Iterable<DiscoveryNode> discoverNodes() {
        return discoveryService.discoverNodes();
    }

    @Override
    public void start() {
        super.start();
        discoveryService.start();
    }

    @Override
    public void destroy() {
        super.destroy();
        discoveryService.stop();
    }

    private DiscoveryService createMulticaster(DiscoveryNode localNode) {
        String group = getOrDefault(ENV_PREFIX, MULTICAST_GROUP, DEFAULT_MULTICAST_GROUP);
        int port = getOrDefault(ENV_PREFIX, MULTICAST_PORT, DEFAULT_MULTICAST_PORT);
        int timeout = getOrDefault(ENV_PREFIX, MULTICAST_TIMEOUT, DEFAULT_MULTICAST_TIMEOUT);
        int timeToLive = getOrDefault(ENV_PREFIX, MULTICAST_TIME_TO_LIVE, DEFAULT_MULTICAST_TIME_TO_LIVE);
        boolean loopbackMode = getOrDefault(ENV_PREFIX, LOOPBACK_MODE_ENABLED, DEFAULT_LOOPBACK_MODE_ENABLED);
        String serviceName = getOrDefault(ENV_PREFIX, SERVICE_NAME, DEFAULT_SERVICE_NAME);
        String trustedInterfaces = getOrNull(ENV_PREFIX, TRUSTED_INTERFACES);

        List<String> interfaces = splitInterfaces(trustedInterfaces);
        return new DiscoveryService(localNode, group, port, timeout, timeToLive, loopbackMode, serviceName, interfaces,
                getLogger());
    }

    private List<String> splitInterfaces(String trustedInterfaces) {
        if (StringUtil.isNullOrEmpty(trustedInterfaces)) {
            return Collections.emptyList();
        }

        String[] tokens = trustedInterfaces.split(",");
        List<String> interfaces = new ArrayList<String>(tokens.length);
        for (String token : tokens) {
            token = token.trim();
            if (!StringUtil.isNullOrEmpty(token)) {
                interfaces.add(token);
            }
        }
        return interfaces;
    }
}
