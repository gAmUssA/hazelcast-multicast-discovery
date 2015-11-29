package com.noctarius.hazelcast.multicast.dbda;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.ASCII;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.HEADER_LENGTH_TCP;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.HEADER_LENGTH_UDP;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.MAX_LENGTH_SERVICE_NAME;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.MAX_LENGTH_TCP_PACKET;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.MAX_LENGTH_UDP_PACKET;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.POSITION_SERVICE_NAME_TCP;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.POSITION_SERVICE_NAME_UDP;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.POSITION_TOTAL_LENGTH;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.PacketType;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.Transport;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.baseHeader;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.getEncrypted;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.getPacketType;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.getVersion;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.readUUID;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.writeUUID;

public abstract class Streamer<P extends Packet> {

    private final PacketType packetType;

    protected Streamer(PacketType packetType) {
        this.packetType = packetType;
    }

    public void write(P packet, ByteBuffer byteBuffer) {
        byte[] header = baseHeader(packetType, packet.isEncrypted());
        byteBuffer.put(header);

        String serviceName = packet.getServiceName();
        byte[] chars = serviceName.getBytes(ASCII);
        if (chars.length > MAX_LENGTH_SERVICE_NAME) {
            throw new IllegalArgumentException("The service name cannot be longer than 20 ASCII chars");
        }

        writeUUID(packet.getUuid(), byteBuffer);

        int positionServiceName;
        int headerLength;
        if (packetType.getTransport() == Transport.UDP) {
            positionServiceName = POSITION_SERVICE_NAME_UDP;
            headerLength = HEADER_LENGTH_UDP;
        } else {
            positionServiceName = POSITION_SERVICE_NAME_TCP;
            headerLength = HEADER_LENGTH_TCP;
        }

        byteBuffer.position(positionServiceName);
        byteBuffer.put(chars);
        byteBuffer.position(headerLength);

        doWrite(packet, byteBuffer);
        int totalLength = byteBuffer.position();

        if (packetType.getTransport() == Transport.UDP) {
            if (totalLength > MAX_LENGTH_UDP_PACKET) {
                throw new IllegalStateException("UDP packet cannot be larger than " + MAX_LENGTH_UDP_PACKET + " bytes");
            }
            byteBuffer.put(POSITION_TOTAL_LENGTH, (byte) totalLength);
        } else {
            if (totalLength > MAX_LENGTH_TCP_PACKET) {
                throw new IllegalStateException("TCP packet cannot be larger than " + MAX_LENGTH_TCP_PACKET + " bytes");
            }
            byteBuffer.putInt(POSITION_TOTAL_LENGTH, totalLength);
        }
    }

    public P read(ByteBuffer byteBuffer) {
        // Skip magic number
        byteBuffer.position(2);

        int tag = byteBuffer.getShort();
        int version = getVersion(tag);
        PacketType packetType = getPacketType(tag);
        boolean encrypted = getEncrypted(tag);

        if (!this.packetType.equals(packetType)) {
            throw new IllegalStateException("Illegal packet type in streamer");
        }

        UUID uuid = readUUID(byteBuffer);

        int totalLength;
        if (packetType.getTransport() == Transport.UDP) {
            totalLength = byteBuffer.get();
        } else {
            totalLength = byteBuffer.getInt();
        }

        byte[] chars = new byte[20];
        byteBuffer.get(chars);
        chars = stripChar0(chars);
        String serviceName = new String(chars, ASCII);

        return doRead(byteBuffer, uuid, version, encrypted, totalLength, serviceName);
    }

    public PacketType packetType() {
        return packetType;
    }

    protected abstract void doWrite(P packet, ByteBuffer byteBuffer);

    protected abstract P doRead(ByteBuffer byteBuffer, UUID uuid, int version, boolean encrypted, int length, String serviceName);

    private byte[] stripChar0(byte[] chars) {
        int firstChar0 = -1;
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == 0) {
                firstChar0 = i;
                break;
            }
        }
        return firstChar0 > -1 ? Arrays.copyOf(chars, firstChar0) : chars;
    }
}
