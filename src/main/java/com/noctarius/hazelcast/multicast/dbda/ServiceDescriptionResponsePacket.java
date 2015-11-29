package com.noctarius.hazelcast.multicast.dbda;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_SERVICE_DESC_RESPONSE;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.UTF8;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.readInetAddress;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.writeInetAddress;

public class ServiceDescriptionResponsePacket
        extends Packet {

    private final InetAddress address;
    private final String json;
    private final int port;

    public ServiceDescriptionResponsePacket(InetAddress address, int port, UUID uuid, String json, boolean encrypted,
                                            String serviceName) {

        super(uuid, encrypted, serviceName, PACKET_TYPE_SERVICE_DESC_RESPONSE);
        this.json = json;
        this.port = port;
        this.address = address;
    }

    public String getJson() {
        return json;
    }

    public int getPort() {
        return port;
    }

    public InetAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServiceDescriptionResponsePacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ServiceDescriptionResponsePacket that = (ServiceDescriptionResponsePacket) o;

        if (port != that.port) {
            return false;
        }
        if (address != null ? !address.equals(that.address) : that.address != null) {
            return false;
        }
        return !(json != null ? !json.equals(that.json) : that.json != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + (json != null ? json.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }

    static class PacketStreamer
            extends Streamer<ServiceDescriptionResponsePacket> {

        PacketStreamer() {
            super(PACKET_TYPE_SERVICE_DESC_RESPONSE);
        }

        @Override
        protected void doWrite(ServiceDescriptionResponsePacket packet, ByteBuffer byteBuffer) {
            writeInetAddress(packet.address, byteBuffer);
            byteBuffer.putInt(packet.port);

            String json = packet.json;
            if (json != null) {
                byte[] chars = json.getBytes(UTF8);
                byteBuffer.put(chars);
            }
        }

        @Override
        protected ServiceDescriptionResponsePacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted,
                                                          int totalLength, String serviceName) {

            InetAddress address = readInetAddress(byteBuffer);
            int port = byteBuffer.getInt();

            byte[] chars = new byte[totalLength - byteBuffer.position()];

            String json = null;
            if (chars.length > 0) {
                byteBuffer.get(chars);
                json = new String(chars, UTF8);
            }

            return new ServiceDescriptionResponsePacket(address, port, uuid, json, encrypted, serviceName);
        }
    }
}
