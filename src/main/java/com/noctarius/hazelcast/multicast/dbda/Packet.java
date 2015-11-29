package com.noctarius.hazelcast.multicast.dbda;

import java.util.UUID;

public abstract class Packet {

    private final boolean encrypted;
    private final String serviceName;
    private final Protocol.PacketType packetType;
    private final UUID uuid;

    Packet(UUID uuid, boolean encrypted, String serviceName, Protocol.PacketType packetType) {
        this.uuid = uuid;
        this.encrypted = encrypted;
        this.serviceName = serviceName;
        this.packetType = packetType;
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean isEncrypted() {
        return encrypted;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Protocol.PacketType getPacketType() {
        return packetType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Packet)) {
            return false;
        }

        Packet packet = (Packet) o;

        if (encrypted != packet.encrypted) {
            return false;
        }
        if (serviceName != null ? !serviceName.equals(packet.serviceName) : packet.serviceName != null) {
            return false;
        }
        if (packetType != packet.packetType) {
            return false;
        }
        return !(uuid != null ? !uuid.equals(packet.uuid) : packet.uuid != null);

    }

    @Override
    public int hashCode() {
        int result = (encrypted ? 1 : 0);
        result = 31 * result + (serviceName != null ? serviceName.hashCode() : 0);
        result = 31 * result + (packetType != null ? packetType.hashCode() : 0);
        result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "encrypted=" + encrypted +
                ", serviceName='" + serviceName + '\'' +
                ", packetType=" + packetType +
                ", uuid=" + uuid +
                '}';
    }
}
