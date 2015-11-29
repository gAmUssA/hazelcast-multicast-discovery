package com.noctarius.hazelcast.multicast.dbda;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_DISCONTINUE;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.readInetAddress;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.writeInetAddress;

public class DiscontinuePacket
        extends Packet {

    private final InetAddress address;

    public DiscontinuePacket(InetAddress address, UUID uuid, boolean encrypted, String serviceName) {
        super(uuid, encrypted, serviceName, PACKET_TYPE_DISCONTINUE);
        this.address = address;
    }

    public InetAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DiscontinuePacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        DiscontinuePacket that = (DiscontinuePacket) o;

        return !(address != null ? !address.equals(that.address) : that.address != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }

    static class PacketStreamer
            extends Streamer<DiscontinuePacket> {

        PacketStreamer() {
            super(PACKET_TYPE_DISCONTINUE);
        }

        @Override
        protected void doWrite(DiscontinuePacket packet, ByteBuffer byteBuffer) {
            writeInetAddress(packet.address, byteBuffer);
        }

        @Override
        protected DiscontinuePacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted, int totalLength,
                                           String serviceName) {

            InetAddress address = readInetAddress(byteBuffer);
            return new DiscontinuePacket(address, uuid, encrypted, serviceName);
        }
    }
}
