package com.noctarius.hazelcast.multicast.dbda;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_SERVICE_DESC_REQUEST;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.UTF8;

public class ServiceDescriptionRequestPacket
        extends Packet {

    private final String filter;

    public ServiceDescriptionRequestPacket(String filter, UUID uuid, boolean encrypted, String serviceName) {
        super(uuid, encrypted, serviceName, PACKET_TYPE_SERVICE_DESC_REQUEST);
        this.filter = filter;
    }

    public String getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServiceDescriptionRequestPacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ServiceDescriptionRequestPacket that = (ServiceDescriptionRequestPacket) o;

        return !(filter != null ? !filter.equals(that.filter) : that.filter != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        return result;
    }

    static class PacketStreamer
            extends Streamer<ServiceDescriptionRequestPacket> {

        PacketStreamer() {
            super(PACKET_TYPE_SERVICE_DESC_REQUEST);
        }

        @Override
        protected void doWrite(ServiceDescriptionRequestPacket packet, ByteBuffer byteBuffer) {
            String filter = packet.filter;
            if (filter != null) {
                byte[] chars = filter.getBytes(UTF8);
                byteBuffer.put(chars);
            }
        }

        @Override
        protected ServiceDescriptionRequestPacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted,
                                                         int totalLength, String serviceName) {

            byte[] chars = new byte[totalLength - byteBuffer.position()];

            String filter = null;
            if (chars.length > 0) {
                byteBuffer.get(chars);
                filter = new String(chars, UTF8);
            }

            return new ServiceDescriptionRequestPacket(filter, uuid, encrypted, serviceName);
        }
    }
}
