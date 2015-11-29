package com.noctarius.hazelcast.multicast.dbda;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.ASCII;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType.PACKET_TYPE_QUERY;

public class QueryPacket
        extends Packet {

    private final short queryId;
    private final String query;

    public QueryPacket(short queryId, String query, UUID uuid, boolean encrypted, String serviceName) {
        super(uuid, encrypted, serviceName, PACKET_TYPE_QUERY);
        this.queryId = queryId;
        this.query = query;
    }

    public short getQueryId() {
        return queryId;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryPacket)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        QueryPacket packet = (QueryPacket) o;

        if (queryId != packet.queryId) {
            return false;
        }
        return !(query != null ? !query.equals(packet.query) : packet.query != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) queryId;
        result = 31 * result + (query != null ? query.hashCode() : 0);
        return result;
    }

    static class PacketStreamer
            extends Streamer<QueryPacket> {

        PacketStreamer() {
            super(PACKET_TYPE_QUERY);
        }

        @Override
        protected void doWrite(QueryPacket packet, ByteBuffer byteBuffer) {
            byteBuffer.put((byte) packet.queryId);

            String query = packet.query;
            if (query != null) {
                byte[] chars = query.getBytes(ASCII);
                byteBuffer.put(chars);
            }
        }

        @Override
        protected QueryPacket doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted, int totalLength,
                                     String serviceName) {

            short queryId = byteBuffer.get();

            byte[] chars = new byte[totalLength - byteBuffer.position()];

            String query = null;
            if (chars.length > 0) {
                byteBuffer.get(chars);
                query = new String(chars, ASCII);
            }

            return new QueryPacket(queryId, query, uuid, encrypted, serviceName);
        }
    }
}
