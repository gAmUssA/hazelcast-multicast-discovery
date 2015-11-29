import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.noctarius.hazelcast.multicast.HazelcastMulticastDiscoveryStrategy;

import java.util.HashMap;
import java.util.Map;

public class Test {

    public static void main(String[] args) {
        Config config = new XmlConfigBuilder().build();
        config.setProperty("hazelcast.discovery.enabled", "true");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);

        DiscoveryConfig discoveryConfig = join.getDiscoveryConfig();

        String className = HazelcastMulticastDiscoveryStrategy.class.getName();

        Map<String, Comparable> configuration = new HashMap<String, Comparable>();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(className, configuration);
        discoveryConfig.addDiscoveryProviderConfig(strategyConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
    }
}
