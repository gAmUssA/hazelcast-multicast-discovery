package com.noctarius.hazelcast.multicast;

import com.noctarius.hazelcast.multicast.dbda.Packet;

import java.io.IOException;
import java.net.Socket;

public class TcpClientResponder
        implements Runnable {

    private final DiscoveryService discoveryService;
    private final PacketHandler packetHandler;
    private final SocketContext socketContext;
    private final Socket socket;

    TcpClientResponder(PacketHandler packetHandler, Socket socket) {
        this.packetHandler = packetHandler;
        this.socket = socket;

        this.discoveryService = packetHandler.getDiscoveryService();
        this.socketContext = discoveryService.createTcpSocketContext(socket);
    }

    public void run() {
        try {
            Packet packet = discoveryService.readPacket(socket);
            packetHandler.handlePacket(packet, socketContext);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
