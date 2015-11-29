package com.noctarius.hazelcast.multicast;

import com.noctarius.hazelcast.multicast.dbda.Packet;
import com.noctarius.hazelcast.multicast.dbda.ServiceDescriptionRequestPacket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

class TcpClientRequester
        implements Runnable {

    private final DiscoveryService discoveryService;
    private final PacketHandler packetHandler;
    private final InetAddress address;
    private final int port;

    TcpClientRequester(PacketHandler packetHandler, InetAddress address, int port) {
        this.packetHandler = packetHandler;
        this.address = address;
        this.port = port;

        this.discoveryService = packetHandler.getDiscoveryService();
    }

    public void run() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(address, port));

            SocketContext socketContext = discoveryService.createTcpSocketContext(socket);

            ServiceDescriptionRequestPacket request = packetHandler.createServiceDescriptionRequest("");
            socketContext.write(request);

            Packet response = discoveryService.readPacket(socket);
            packetHandler.handlePacket(response, socketContext);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
