package com.noctarius.hazelcast.multicast.dbda;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Protocol {

    private static final int SHIFT_VERSION = 5;
    private static final int SHIFT_PACKET_TYPE = 1;
    private static final int SHIFT_ENCRYPTED = 0;

    private static final int MASK_VERSION = 0x7;
    private static final int MASK_PACKET_TYPE = 0xF;
    private static final int MASK_ENCRYPTED = 0x1;

    private static final byte[] MAGIC_NUMBER = {(byte) 0xDB, (byte) 0xDA};
    private static final byte[] BASE_HEADER = buildBaseHeader();

    private static Map<PacketType, Streamer<?>> STREAMERS;

    static final Charset ASCII = Charset.forName("ASCII");
    static final Charset UTF8 = Charset.forName("UTF-8");

    static final int HEADER_LENGTH_UDP = 41;
    static final int HEADER_LENGTH_TCP = 44;

    static final int IP_VERSION_TYPE_4 = 0x1;
    static final int IP_VERSION_TYPE_6 = 0x2;

    public static final int POSITION_SERVICE_NAME_UDP = 21;
    public static final int POSITION_SERVICE_NAME_TCP = 24;
    public static final int POSITION_TOTAL_LENGTH = 20;

    public static final int VERSION = 1;
    public static final int MAX_LENGTH_UDP_PACKET = 0xFF;
    public static final int MAX_LENGTH_TCP_PACKET = 0xFFFFFF;
    public static final int MAX_LENGTH_SERVICE_NAME = 20;

    public static final int BYTE_LENGTH_UUID = 16;
    public static final int BYTE_LENGTH_IPV6 = 16;
    public static final int BYTE_LENGTH_IPV4 = 4;

    static {
        Map<PacketType, Streamer<?>> streamers = new HashMap<PacketType, Streamer<?>>();
        registerStreamer(new AnnouncementPacket.PacketStreamer(), streamers);
        registerStreamer(new DiscontinuePacket.PacketStreamer(), streamers);
        registerStreamer(new QueryPacket.PacketStreamer(), streamers);
        registerStreamer(new QueryResponsePacket.PacketStreamer(), streamers);
        registerStreamer(new ServiceDescriptionRequestPacket.PacketStreamer(), streamers);
        registerStreamer(new ServiceDescriptionResponsePacket.PacketStreamer(), streamers);
        STREAMERS = streamers;
    }

    private Protocol() {
    }

    public static <P extends Packet> Streamer<P> findStreamer(ByteBuffer byteBuffer) {
        int tag = byteBuffer.getShort(2);
        PacketType packetType = getPacketType(tag);
        return (Streamer<P>) STREAMERS.get(packetType);
    }

    public static <P extends Packet> Streamer<P> findStreamer(P packet) {
        return (Streamer<P>) STREAMERS.get(packet.getPacketType());
    }

    public static int getVersion(int tag) {
        tag = tag >> 8;
        return ((tag >> SHIFT_VERSION) & MASK_VERSION);
    }

    public static PacketType getPacketType(int tag) {
        tag = tag >> 8;
        int id = ((tag >> SHIFT_PACKET_TYPE) & MASK_PACKET_TYPE);
        return PacketType.byId(id);
    }

    public static boolean getEncrypted(int tag) {
        tag = tag >> 8;
        return ((tag >> SHIFT_ENCRYPTED) & MASK_ENCRYPTED) == 1;
    }

    public static boolean fullyRead(ByteBuffer byteBuffer) {
        if (byteBuffer.position() < POSITION_TOTAL_LENGTH) {
            return false;
        }

        int tag = byteBuffer.getShort(2);
        PacketType packetType = getPacketType(tag);

        int totalLength;
        if (packetType.getTransport() == Protocol.Transport.UDP) {
            if (byteBuffer.position() < POSITION_TOTAL_LENGTH + 1) {
                return false;
            }
            totalLength = byteBuffer.get(POSITION_TOTAL_LENGTH);
        } else {
            if (byteBuffer.position() < POSITION_TOTAL_LENGTH + 4) {
                return false;
            }
            totalLength = byteBuffer.getInt(POSITION_TOTAL_LENGTH);
        }

        return byteBuffer.position() >= totalLength;
    }

    public static void writeInetAddress(InetAddress address, ByteBuffer byteBuffer) {
        byte[] addressBytes = address.getAddress();
        byteBuffer.put((byte) (addressBytes.length == BYTE_LENGTH_IPV4 ? IP_VERSION_TYPE_4 : IP_VERSION_TYPE_6));
        byteBuffer.put(addressBytes);
    }

    public static InetAddress readInetAddress(ByteBuffer byteBuffer) {
        int ipType = byteBuffer.get();

        byte[] addressBytes;
        if (ipType == IP_VERSION_TYPE_4) {
            addressBytes = new byte[BYTE_LENGTH_IPV4];
        } else {
            addressBytes = new byte[BYTE_LENGTH_IPV6];
        }

        byteBuffer.get(addressBytes);

        try {
            return InetAddress.getByAddress(addressBytes);
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Unable to read announcement packet", e);
        }
    }

    public static void writeUUID(UUID uuid, ByteBuffer byteBuffer) {
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        byteBuffer.putLong(uuid.getMostSignificantBits());
    }

    public static UUID readUUID(ByteBuffer byteBuffer) {
        long leastSignificantBits = byteBuffer.getLong();
        long mostSignificantBits = byteBuffer.getLong();
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    static byte[] baseHeader(PacketType packetType, boolean encrypted) {
        byte[] header = Arrays.copyOf(BASE_HEADER, BASE_HEADER.length);
        header[2] |= packetType.getId() << SHIFT_PACKET_TYPE;
        header[2] |= (encrypted ? 1 : 0) << SHIFT_ENCRYPTED;
        return header;
    }

    private static byte[] buildBaseHeader() {
        byte[] content = new byte[4];
        System.arraycopy(MAGIC_NUMBER, 0, content, 0, 2);
        content[2] = VERSION << SHIFT_VERSION;
        return content;
    }

    private static void registerStreamer(Streamer<?> streamer, Map<PacketType, Streamer<?>> streamers) {
        streamers.put(streamer.packetType(), streamer);
    }

    public enum PacketType {
        PACKET_TYPE_ANNOUNCEMENT(1, Transport.UDP),
        PACKET_TYPE_DISCONTINUE(2, Transport.UDP),
        PACKET_TYPE_QUERY(3, Transport.UDP),
        PACKET_TYPE_QUERY_RESPONSE(4, Transport.UDP),
        PACKET_TYPE_SERVICE_DESC_REQUEST(5, Transport.TCP),
        PACKET_TYPE_SERVICE_DESC_RESPONSE(6, Transport.TCP);

        private int id;
        private Transport transport;

        PacketType(int id, Transport transport) {
            this.id = id;
            this.transport = transport;
        }

        public int getId() {
            return id;
        }

        public Transport getTransport() {
            return transport;
        }

        public static PacketType byId(int id) {
            for (PacketType packetType : values()) {
                if (packetType.getId() == id) {
                    return packetType;
                }
            }
            throw new IllegalStateException("Illegal packet type id received");
        }
    }

    public enum Transport {
        UDP,
        TCP
    }
}
