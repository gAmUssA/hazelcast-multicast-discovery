package com.noctarius.hazelcast.multicast.dbda;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_ANNOUNCEMENT;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.readInetAddress;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.writeInetAddress;

public class AnnouncementPacket
        extends Packet {

    private final InetAddress address;
    private final int port;

    public AnnouncementPacket(InetAddress address, int port, UUID uuid, boolean encrypted, String serviceName) {
        super(uuid, encrypted, serviceName, PACKET_TYPE_ANNOUNCEMENT);
        this.address = address;
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AnnouncementPacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        AnnouncementPacket that = (AnnouncementPacket) o;

        if (port != that.port) {
            return false;
        }
        return !(address != null ? !address.equals(that.address) : that.address != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }

    static class PacketStreamer
            extends Streamer<AnnouncementPacket> {

        PacketStreamer() {
            super(PACKET_TYPE_ANNOUNCEMENT);
        }

        @Override
        protected void doWrite(AnnouncementPacket packet, ByteBuffer byteBuffer) {
            writeInetAddress(packet.address, byteBuffer);
            byteBuffer.putInt(packet.port);
        }

        @Override
        protected AnnouncementPacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted, int totalLength,
                                            String serviceName) {

            InetAddress address = readInetAddress(byteBuffer);
            int port = byteBuffer.getInt();

            return new AnnouncementPacket(address, port, uuid, encrypted, serviceName);
        }
    }
}
