package com.noctarius.hazelcast.multicast;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.noctarius.hazelcast.multicast.dbda.AnnouncementPacket;
import com.noctarius.hazelcast.multicast.dbda.Packet;
import com.noctarius.hazelcast.multicast.dbda.Protocol;
import com.noctarius.hazelcast.multicast.dbda.QueryPacket;
import com.noctarius.hazelcast.multicast.dbda.Streamer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.noctarius.hazelcast.multicast.dbda.Protocol.MAX_LENGTH_TCP_PACKET;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.MAX_LENGTH_UDP_PACKET;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.findStreamer;
import static com.noctarius.hazelcast.multicast.dbda.Protocol.fullyRead;

class DiscoveryService {

    private final Map<UUID, DiscoveryNode> discoveredNodes = new ConcurrentHashMap<UUID, DiscoveryNode>();

    private final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    private final ThreadLocal<ByteBuffer> tcpByteBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(MAX_LENGTH_TCP_PACKET);
        }
    };

    private final ThreadLocal<ByteBuffer> udpByteBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(MAX_LENGTH_UDP_PACKET);
        }
    };

    private final SocketContext udpSocketContext = new SocketContext() {
        public void write(Packet packet) {
            sendUdpPacket(packet);
        }
    };

    private final ReentrantLock discoveredNodesLock = new ReentrantLock();
    private final Condition discoveredNodesCondition = discoveredNodesLock.newCondition();

    private final List<String> trustedInterfaces;
    private final InetAddress joinGroupAddress;
    private final PacketHandler packetHandler;
    private final DiscoveryNode localNode;
    private final String serviceName;
    private final ILogger logger;
    private final int port;

    private final MulticastSocket multicastSocket;
    private final ServerSocket deviceSocket;

    private final Thread multicastServer;
    private final Thread tcpServer;

    private final ExecutorService executorService;

    private final UUID uuid = UUID.randomUUID();

    private final AtomicBoolean shutdown = new AtomicBoolean();

    DiscoveryService(DiscoveryNode localNode, String group, int port, int timeout, int timeToLive, boolean loopbackMode,
                     String serviceName, List<String> trustedInterfaces, ILogger logger) {

        try {
            this.multicastSocket = createMulticastSocket(localNode, port, timeout, timeToLive, loopbackMode);
            this.deviceSocket = createDeviceSocket(localNode);
            this.joinGroupAddress = InetAddress.getByName(group);

        } catch (IOException e) {
            throw new RuntimeException("Problem configuring the multicast socket", e);
        }

        this.trustedInterfaces = trustedInterfaces;
        this.serviceName = serviceName;
        this.localNode = localNode;
        this.logger = logger;
        this.port = port;

        this.packetHandler = new PacketHandler(this);

        this.multicastServer = new Thread(new MulticastServer());
        this.tcpServer = new Thread(new TcpServer());

        this.executorService = Executors.newFixedThreadPool(5, new ClientThreadFactory());
    }

    public Iterable<DiscoveryNode> discoverNodes() {
        System.out.println("Discover nodes request");
        if (discoveredNodes.size() == 0) {
            discoveredNodesLock.lock();
            try {
                discoveredNodesCondition.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                discoveredNodesLock.unlock();
            }
        }

        return new ArrayList<DiscoveryNode>(discoveredNodes.values());
    }

    void start() {
        try {
            multicastSocket.joinGroup(joinGroupAddress);
        } catch (IOException e) {
            throw new RuntimeException("Problem starting the multicast socket", e);
        }

        multicastServer.start();
        tcpServer.start();
    }

    void stop() {
        shutdown.set(true);
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        multicastSocket.close();
        try {
            deviceSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    InetAddress getInetAddress(DiscoveryNode node) {
        try {
            return node.getPrivateAddress().getInetAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    String getServiceName() {
        return serviceName;
    }

    UUID getUuid() {
        return uuid;
    }

    boolean alreadyKnown(UUID uuid) {
        return discoveredNodes.containsKey(uuid);
    }

    InetAddress getTcpAddress() {
        return deviceSocket.getInetAddress();
    }

    int getTcpPort() {
        return deviceSocket.getLocalPort();
    }

    InetAddress getUdpAddress() {
        return multicastSocket.getInetAddress();
    }

    int getUdpPort() {
        return multicastSocket.getLocalPort();
    }

    SocketContext createUdpSocketContext() {
        return udpSocketContext;
    }

    SocketContext createTcpSocketContext(Socket socket) {
        return new TcpSocketContext(socket, this);
    }

    void submitTask(Runnable runnable) {
        executorService.submit(runnable);
    }

    void warning(String message) {
        warning(message, null);
    }

    void warning(String message, Throwable throwable) {
        logger.warning(message, throwable);
    }

    void nodeDiscovered(UUID uuid, DiscoveryNode discoveryNode) {
        logger.info("Discovered new node: address=" + discoveryNode.getPrivateAddress());

        discoveredNodes.put(uuid, discoveryNode);

        discoveredNodesLock.lock();
        try {
            discoveredNodesCondition.signalAll();
        } finally {
            discoveredNodesLock.unlock();
        }
    }

    DiscoveryNode getLocalNode() {
        return localNode;
    }

    Packet readPacket(Socket socket)
            throws IOException {

        ByteBuffer byteBuffer = tcpByteBuffer.get();
        byteBuffer.clear();

        InputStream is = socket.getInputStream();
        fillByteBuffer(is, byteBuffer);

        Streamer<Packet> streamer = findStreamer(byteBuffer);
        return streamer.read(byteBuffer);
    }

    private Packet readPacket(DatagramPacket datagramPacket) {
        if (datagramPacket.getLength() <= 0) {
            return null;
        }

        byte[] data = datagramPacket.getData();
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byteBuffer.limit(datagramPacket.getLength());

        Streamer<Packet> streamer = findStreamer(byteBuffer);
        Packet packet = streamer.read(byteBuffer);

        datagramPacket.setLength(data.length);
        return packet;
    }

    private void fillByteBuffer(InputStream is, ByteBuffer byteBuffer)
            throws IOException {

        byte[] buffer = this.buffer.get();
        do {
            int count = is.read(buffer);
            byteBuffer.put(buffer, 0, count);
        } while (!fullyRead(byteBuffer));
    }

    private void sendTcpPacket(Packet packet, Socket socket) {
        try {
            Protocol.PacketType packetType = packet.getPacketType();
            ByteBuffer byteBuffer = getByteBuffer(packetType);

            Streamer<Packet> streamer = findStreamer(packet);
            streamer.write(packet, byteBuffer);

            OutputStream os = socket.getOutputStream();
            os.write(byteBuffer.array(), 0, byteBuffer.position());
            os.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendUdpPacket(Packet packet) {
        Protocol.PacketType packetType = packet.getPacketType();
        ByteBuffer byteBuffer = getByteBuffer(packetType);

        Streamer<Packet> streamer = findStreamer(packet);
        streamer.write(packet, byteBuffer);

        DatagramPacket datagramPacket = new DatagramPacket(byteBuffer.array(), byteBuffer.position());
        datagramPacket.setAddress(joinGroupAddress);
        datagramPacket.setPort(port);
        datagramPacket.setData(byteBuffer.array(), 0, byteBuffer.position());
        try {
            multicastSocket.send(datagramPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ServerSocket createDeviceSocket(DiscoveryNode localNode)
            throws IOException {

        ServerSocket serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);

        InetAddress address = getInetAddress(localNode);
        serverSocket.bind(new InetSocketAddress(address, 0));

        return serverSocket;
    }

    private MulticastSocket createMulticastSocket(DiscoveryNode localNode, int port, int timeout, int ttl, boolean loopbackMode)
            throws IOException {

        MulticastSocket multicastSocket = new MulticastSocket(null);
        multicastSocket.setReuseAddress(true);
        multicastSocket.bind(new InetSocketAddress(port));

        InetAddress address = getInetAddress(localNode);

        if (!address.isLoopbackAddress()) {
            multicastSocket.setInterface(address);
        } else if (loopbackMode) {
            multicastSocket.setLoopbackMode(true);
            multicastSocket.setInterface(address);
        }

        multicastSocket.setReceiveBufferSize(64 * 1024);
        multicastSocket.setSendBufferSize(64 * 1024);
        multicastSocket.setTimeToLive(ttl);
        multicastSocket.setSoTimeout(timeout);

        return multicastSocket;
    }

    private ByteBuffer getByteBuffer(Protocol.PacketType packetType) {
        ByteBuffer byteBuffer;
        if (packetType.getTransport() == Protocol.Transport.TCP) {
            byteBuffer = tcpByteBuffer.get();
        } else {
            byteBuffer = udpByteBuffer.get();
        }
        byteBuffer.clear();
        return byteBuffer;
    }

    private void sendInitialQuery(short queryId) {
        QueryPacket packet = packetHandler.createQuery(queryId, "all");
        udpSocketContext.write(packet);
    }

    private void sendAnnouncement() {
        InetAddress address = deviceSocket.getInetAddress();
        int port = deviceSocket.getLocalPort();

        AnnouncementPacket packet = packetHandler.createAnnouncement(address, port);
        udpSocketContext.write(packet);
    }

    private class MulticastServer
            implements Runnable {

        private final byte[] data = new byte[MAX_LENGTH_UDP_PACKET];
        private final DatagramPacket datagramPacket = new DatagramPacket(data, MAX_LENGTH_UDP_PACKET);

        private short queryId = 0;

        public void run() {
            sendInitialQuery(nextQueryId());
            long lastAnnouncementSent = System.currentTimeMillis();

            while (true) {
                if (shutdown.get()) {
                    break;
                }

                if (lastAnnouncementSent + 10000 < System.currentTimeMillis()) {
                    lastAnnouncementSent = System.currentTimeMillis();
                    System.out.println("Send announcement");
                    sendAnnouncement();
                }

                try {
                    Packet packet = receive();
                    if (packet != null) {
                        packetHandler.handlePacket(packet, udpSocketContext);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Weird");
            // todo broadcast discontinue
        }

        private short nextQueryId() {
            short queryId = ++this.queryId;
            if (queryId == 255) {
                queryId = 1;
            }
            return queryId;
        }

        private Packet receive()
                throws IOException {

            datagramPacket.setData(data);
            datagramPacket.setAddress(joinGroupAddress);
            datagramPacket.setPort(port);
            datagramPacket.setLength(MAX_LENGTH_UDP_PACKET);

            try {
                multicastSocket.receive(datagramPacket);
            } catch (SocketTimeoutException e) {
                return null;
            }

            Packet packet = readPacket(datagramPacket);
            Arrays.fill(data, (byte) 0);
            return packet;
        }
    }

    private class TcpServer
            implements Runnable {

        public void run() {
            while (true) {
                if (shutdown.get()) {
                    return;
                }

                try {
                    Socket socket = deviceSocket.accept();
                    System.out.println("Received device lookup request");
                    executorService.submit(new TcpClientResponder(packetHandler, socket));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientThreadFactory
            implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(1);

        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, serviceName + "-tcp-client-" + counter.getAndIncrement());
        }
    }

    private static class TcpSocketContext
            implements SocketContext {

        private final Socket socket;
        private final DiscoveryService discoveryService;

        private TcpSocketContext(Socket socket, DiscoveryService discoveryService) {
            this.socket = socket;
            this.discoveryService = discoveryService;
        }

        public void write(Packet packet) {
            discoveryService.sendTcpPacket(packet, socket);
        }
    }
}
