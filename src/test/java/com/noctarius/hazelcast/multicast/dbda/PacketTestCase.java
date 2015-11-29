package com.noctarius.hazelcast.multicast.dbda;

import org.junit.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.noctarius.hazelcast.multicast.MulticastProperties.DEFAULT_SERVICE_NAME;
import static org.junit.Assert.assertEquals;

public class PacketTestCase {

    @Test
    public void test_announce_packet_ipv4()
            throws Exception {

        InetAddress address = Inet4Address.getByName("127.0.0.1");
        UUID uuid = UUID.randomUUID();

        AnnouncementPacket packet = new AnnouncementPacket(address, 1234, uuid, false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<AnnouncementPacket> streamer = new AnnouncementPacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        AnnouncementPacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_announce_packet_ipv6()
            throws Exception {

        InetAddress address = Inet6Address.getByName("::1");
        UUID uuid = UUID.randomUUID();

        AnnouncementPacket packet = new AnnouncementPacket(address, 1234, uuid, false, "asdfghjklasdfghjklas");

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<AnnouncementPacket> streamer = new AnnouncementPacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        AnnouncementPacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_discontinue_packet_ipv4()
            throws Exception {

        InetAddress address = Inet4Address.getByName("127.0.0.1");
        DiscontinuePacket packet = new DiscontinuePacket(address, UUID.randomUUID(), false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<DiscontinuePacket> streamer = new DiscontinuePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        DiscontinuePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_discontinue_packet_ipv6()
            throws Exception {

        InetAddress address = Inet6Address.getByName("::1");
        DiscontinuePacket packet = new DiscontinuePacket(address, UUID.randomUUID(), false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<DiscontinuePacket> streamer = new DiscontinuePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        DiscontinuePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_query_packet()
            throws Exception {

        QueryPacket packet = new QueryPacket((short) 1, "port=5701", UUID.randomUUID(), false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<QueryPacket> streamer = new QueryPacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        QueryPacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_query_response_packet_ipv4()
            throws Exception {

        InetAddress address = Inet4Address.getByName("127.0.0.1");
        UUID uuid = UUID.randomUUID();

        QueryResponsePacket packet = new QueryResponsePacket((short) 1, uuid, address, 1234, false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<QueryResponsePacket> streamer = new QueryResponsePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        QueryResponsePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_query_response_packet_ipv6()
            throws Exception {

        InetAddress address = Inet6Address.getByName("::1");
        UUID uuid = UUID.randomUUID();

        QueryResponsePacket packet = new QueryResponsePacket((short) 1, uuid, address, 1234, false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_UDP_PACKET);
        Streamer<QueryResponsePacket> streamer = new QueryResponsePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        QueryResponsePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_service_description_request_packet()
            throws Exception {

        ServiceDescriptionRequestPacket packet = new ServiceDescriptionRequestPacket("öäüßasdf", UUID.randomUUID(), false,
                "asdfghjklasdfghjklas");

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_TCP_PACKET);
        Streamer<ServiceDescriptionRequestPacket> streamer = new ServiceDescriptionRequestPacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        ServiceDescriptionRequestPacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_service_description_response_packet_ipv4()
            throws Exception {

        InetAddress address = Inet4Address.getByName("127.0.0.1");
        ServiceDescriptionResponsePacket packet = //
                new ServiceDescriptionResponsePacket(address, 1234, UUID.randomUUID(), "öäüßasdf", false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_TCP_PACKET);
        Streamer<ServiceDescriptionResponsePacket> streamer = new ServiceDescriptionResponsePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        ServiceDescriptionResponsePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }

    @Test
    public void test_service_description_response_packet_ipv6()
            throws Exception {

        InetAddress address = Inet6Address.getByName("::1");
        ServiceDescriptionResponsePacket packet = //
                new ServiceDescriptionResponsePacket(address, 1234, UUID.randomUUID(), "öäüßasdf", false, DEFAULT_SERVICE_NAME);

        ByteBuffer byteBuffer = ByteBuffer.allocate(Protocol.MAX_LENGTH_TCP_PACKET);
        Streamer<ServiceDescriptionResponsePacket> streamer = new ServiceDescriptionResponsePacket.PacketStreamer();

        streamer.write(packet, byteBuffer);
        byteBuffer.flip();

        ServiceDescriptionResponsePacket result = streamer.read(byteBuffer);
        assertEquals(packet, result);
    }
}
