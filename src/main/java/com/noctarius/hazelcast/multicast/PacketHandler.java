package com.noctarius.hazelcast.multicast;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.noctarius.hazelcast.multicast.dbda.AnnouncementPacket;
import com.noctarius.hazelcast.multicast.dbda.DiscontinuePacket;
import com.noctarius.hazelcast.multicast.dbda.Packet;
import com.noctarius.hazelcast.multicast.dbda.QueryPacket;
import com.noctarius.hazelcast.multicast.dbda.QueryResponsePacket;
import com.noctarius.hazelcast.multicast.dbda.ServiceDescriptionRequestPacket;
import com.noctarius.hazelcast.multicast.dbda.ServiceDescriptionResponsePacket;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class PacketHandler {

    private final DiscoveryService discoveryService;
    private final String serviceName;
    private final UUID uuid;

    private final InetAddress tcpAddress;
    private final int tcpPort;

    private final InetAddress udpAddress;
    private final int udpPort;

    PacketHandler(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;

        this.serviceName = discoveryService.getServiceName();
        this.uuid = discoveryService.getUuid();

        this.tcpAddress = discoveryService.getTcpAddress();
        this.tcpPort = discoveryService.getTcpPort();

        this.udpAddress = discoveryService.getUdpAddress();
        this.udpPort = discoveryService.getUdpPort();
    }

    DiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    void handlePacket(Packet packet, SocketContext socketContext) {
        if (uuid.equals(packet.getUuid())) {
            return;
        }

        if (!serviceName.equals(packet.getServiceName())) {
            return;
        }

        if (packet instanceof AnnouncementPacket) {
            handleAnnouncement((AnnouncementPacket) packet);
        } else if (packet instanceof DiscontinuePacket) {
            System.out.println("Discontinue packet received");
        } else if (packet instanceof QueryPacket) {
            System.out.println("Query packet received");
            handleQuery((QueryPacket) packet, socketContext);
        } else if (packet instanceof QueryResponsePacket) {
            System.out.println("QueryResponse packet received");
            handleQueryResponse((QueryResponsePacket) packet);
        } else if (packet instanceof ServiceDescriptionRequestPacket) {
            handleServiceDescriptionRequest((ServiceDescriptionRequestPacket) packet, socketContext);
        } else if (packet instanceof ServiceDescriptionResponsePacket) {
            handleServiceDescriptionResponse((ServiceDescriptionResponsePacket) packet);
        }
    }

    AnnouncementPacket createAnnouncement(InetAddress address, int port) {
        return new AnnouncementPacket(address, port, uuid, false, serviceName);
    }

    QueryPacket createQuery(short queryId, String query) {
        return new QueryPacket(queryId, query, uuid, false, serviceName);
    }

    QueryResponsePacket createQueryResponse(short queryId) {
        return new QueryResponsePacket(queryId, uuid, tcpAddress, tcpPort, false, serviceName);
    }

    ServiceDescriptionRequestPacket createServiceDescriptionRequest(String filter) {
        return new ServiceDescriptionRequestPacket(filter, uuid, false, serviceName);
    }

    ServiceDescriptionResponsePacket createServiceDescriptionResponsePacket(InetAddress address, int port, String json) {
        return new ServiceDescriptionResponsePacket(address, port, uuid, json, false, serviceName);
    }

    private void handleAnnouncement(AnnouncementPacket packet) {
        InetAddress address = packet.getAddress();
        int port = packet.getPort();
        submitServiceDescriptionRequest(packet.getUuid(), address, port);
    }

    private void handleQuery(QueryPacket packet, SocketContext socketContext) {
        short queryId = packet.getQueryId();

        QueryResponsePacket response = createQueryResponse(queryId);
        socketContext.write(response);
    }

    private void handleQueryResponse(QueryResponsePacket packet) {
        InetAddress address = packet.getAddress();
        int port = packet.getPort();
        submitServiceDescriptionRequest(packet.getUuid(), address, port);
    }

    private void handleServiceDescriptionRequest(ServiceDescriptionRequestPacket packet, SocketContext socketContext) {
        String filter = packet.getFilter();
        // TODO filtering

        DiscoveryNode localNode = discoveryService.getLocalNode();
        JsonObject json = convertMemberAttributes(localNode);
        String jsonValue = json.toString();
        InetAddress address = discoveryService.getInetAddress(localNode);
        int port = localNode.getPrivateAddress().getPort();

        ServiceDescriptionResponsePacket response = createServiceDescriptionResponsePacket(address, port, jsonValue);
        socketContext.write(response);
    }

    private void handleServiceDescriptionResponse(ServiceDescriptionResponsePacket packet) {
        JsonObject json = JsonObject.readFrom(packet.getJson());

        Map<String, Object> properties = new HashMap<String, Object>();
        for (String key : json.names()) {
            JsonValue value = json.get(key);
            if (value.isBoolean()) {
                properties.put(key, value.asBoolean());
            } else if (value.isNumber()) {
                properties.put(key, value.asLong());
            } else if (value.isString()) {
                properties.put(key, value.asString());
            } else {
                discoveryService.warning("Unexpected value in node properties: " + key + " => " + value);
            }
        }

        UUID uuid = packet.getUuid();
        InetAddress inetAddress = packet.getAddress();
        int port = packet.getPort();
        Address address = new Address(inetAddress, port);

        discoveryService.nodeDiscovered(uuid, new SimpleDiscoveryNode(address, properties));
    }

    private void submitServiceDescriptionRequest(UUID uuid, InetAddress address, int port) {
        if (discoveryService.alreadyKnown(uuid)) {
            return;
        }

        System.out.println("Received query response");
        discoveryService.submitTask(new TcpClientRequester(this, address, port));
    }

    private JsonObject convertMemberAttributes(DiscoveryNode localNode) {
        JsonObject json = new JsonObject();
        for (Map.Entry<String, Object> entry : localNode.getProperties().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Boolean) {
                json.add(key, (Boolean) value);
            } else if (value instanceof Number) {
                json.add(key, ((Number) value).longValue());
            } else if (value instanceof String) {
                json.add(key, (String) value);
            }
        }
        return json;
    }
}
