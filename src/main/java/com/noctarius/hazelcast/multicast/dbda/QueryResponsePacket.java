package com.noctarius.hazelcast.multicast.dbda;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_QUERY_RESPONSE;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.readInetAddress;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.writeInetAddress;

public class QueryResponsePacket
        extends Packet {

    private final short queryId;
    private final InetAddress address;
    private final int port;

    public QueryResponsePacket(short queryId, UUID uuid, InetAddress address, int port, boolean encrypted, String serviceName) {
        super(uuid, encrypted, serviceName, PACKET_TYPE_QUERY_RESPONSE);
        this.queryId = queryId;
        this.address = address;
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public short getQueryId() {
        return queryId;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryResponsePacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        QueryResponsePacket that = (QueryResponsePacket) o;

        if (queryId != that.queryId) {
            return false;
        }
        if (port != that.port) {
            return false;
        }
        return !(address != null ? !address.equals(that.address) : that.address != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) queryId;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }

    static class PacketStreamer
            extends Streamer<QueryResponsePacket> {

        PacketStreamer() {
            super(PACKET_TYPE_QUERY_RESPONSE);
        }

        @Override
        protected void doWrite(QueryResponsePacket packet, ByteBuffer byteBuffer) {
            byteBuffer.put((byte) packet.queryId);
            writeInetAddress(packet.address, byteBuffer);
            byteBuffer.putInt(packet.port);
        }

        @Override
        protected QueryResponsePacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted, int totalLength,
                                             String serviceName) {

            short queryId = byteBuffer.get();
            InetAddress address = readInetAddress(byteBuffer);
            int port = byteBuffer.getInt();
            return new QueryResponsePacket(queryId, uuid, address, port, encrypted, serviceName);
        }
    }
}
