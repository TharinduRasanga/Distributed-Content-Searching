package com.distributed.fs.node;

import com.distributed.fs.Constants;
import com.distributed.fs.filesystem.FileManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.distributed.fs.Util.*;
import static com.distributed.fs.Constants.*;

@Slf4j
public class Node {

    private NodeIdentity nodeIdentity;
    private NodeIdentity bootstrapIdentity;
    private Set<NodeIdentity> peersToConnect = new HashSet<>();
    private Set<NodeIdentity> peers = new HashSet<>();
    private final ExecutorService executorService;
    
    private final FileManager fileManager;
    
    private final String fileServerPort;

    public Node(NodeIdentity bootstrapIdentity, FileManager fileManager, String fileServerPort) {
        this.fileManager = fileManager;
        this.fileServerPort = fileServerPort;
        assignNewIdentity();
        this.bootstrapIdentity = bootstrapIdentity;
        executorService = Executors.newFixedThreadPool(2);
        connectToBootstrapServer();
        listenToIncomingRequests();
        connectToPeers();
    }

    private void listenToIncomingRequests() {
        executorService.submit(this::listenAndAct);
    }

    private void listenAndAct() {

        try {
            DatagramSocket serverSocket = new DatagramSocket(nodeIdentity.getPort());
            log.info("Started listening on '" + nodeIdentity.getPort() + "' for incoming data...");

            while (true) {
                byte[] buffer = new byte[65536];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(incoming);

                byte[] data = incoming.getData();
                String incomingMessage = new String(data, 0, incoming.getLength());

                //incomingMessage = 0047 SER 129.82.62.142 5070 "Lord of the rings" 3

                String[] response = splitIncomingMessage(incomingMessage);
                byte[] sendData = null;

                InetAddress responseAddress = incoming.getAddress();
                int responsePort = incoming.getPort();

                if (response.length >= 6 && Constants.SER.equals(response[1])) {

                } else if (response.length >= 4 && JOIN.equals(response[1])) {
                    sendData = processJoinRequest(incomingMessage, response, responseAddress, responsePort);
                } else if (response.length >= 4 && LEAVE.equals(response[1])) {
                    sendData = processLeaveRequest(incomingMessage, response, responseAddress, responsePort);
                } else if (response.length >= 4 && SEROK.equals(response[1])) {

                } else if (response.length >= 4 && RANK.equals(response[1])) {

                } else if (response.length >= 4 && COM.equals(response[1])) {

                } else if (response.length >= 4 && COMRPLY.equals(response[1])) {

                } else if (PUBLISH.equals(response[1])) {
                    sendData = processPublish(incomingMessage, response, responseAddress, responsePort);
                }

                if (sendData != null) {
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress,
                            responsePort);
                    serverSocket.send(sendPacket);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] processPublish(String incomingMessage, String[] response, InetAddress responseAddress, int responsePort) {
        byte[] sendData;
        log.info("RECEIVE: publish received from '" + responseAddress + ":" + responsePort +
                "' as '" + incomingMessage + "'");
        String fileName = response[2];
        String location = response[3] + ":" + response[4] + ":" + response[5];
        fileManager.addToFileTable(fileName, location);
        sendData = prependLengthToMessage("PUBLISH OK").getBytes();
        log.info("SENT: publish ok sent to '" + responseAddress + ":" + responsePort +
                "' as '" + incomingMessage + "'");
        return sendData;
    }

    private byte[] processLeaveRequest(String incomingMessage, String[] response, InetAddress responseAddress, int responsePort) {
        byte[] sendData;
        log.info("RECEIVE: Leave query received from '" + responseAddress + ":" + responsePort +
                "' as '" + incomingMessage + "'");
        NodeIdentity node = NodeIdentity.of(responseAddress.getHostAddress(),
                Integer.parseInt(response[3]));
        peers.remove(node);
        //TODO
        fileManager.removeFromFileTable(node);
        sendData = prependLengthToMessage("LEAVEOK 0").getBytes();
        return sendData;
    }

    private byte[] processJoinRequest(String incomingMessage, String[] response, InetAddress responseAddress, int responsePort) {
        byte[] sendData;
        log.info("RECEIVE: Join query received from '" + responseAddress + ":" + responsePort +
                "' as '" + incomingMessage + "'");
        peers.add(NodeIdentity.of(responseAddress.getHostAddress(), Integer.parseInt(response[3])));
        sendData = prependLengthToMessage("JOINOK 0").getBytes();
        return sendData;
    }

    public void connectToBootstrapServer() {
        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIdentity.getIpAddress());
            System.out.println(bootstrapHost);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = prependLengthToMessage("REG " + nodeIdentity.getIpAddress() + " " + nodeIdentity.getPort() + " " + "test");
            byte[] sendData = message.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, bootstrapHost, 55555);
            log.info("SEND: Bootstrap server register message '" + message + "'");
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);

            String responseMessage = new String(receivePacket.getData()).trim();
            log.info("RECEIVE: Bootstrap server response '" + responseMessage + "'");

            String[] response = responseMessage.split(" ");

            if (response.length >= 4 && Constants.REG_OK.equals(response[1])) {
                if (2 == Integer.parseInt(response[2]) || 1 == Integer.parseInt(response[2])) {
                    for (int i = 3; i < response.length; ) {
                        NodeIdentity neighbour = NodeIdentity.of(response[i], Integer.parseInt(response[i + 1]));
                        peersToConnect.add(neighbour);
                        i = i + 2;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void connectToPeers() {
        executorService.submit(this::tryToConnectPeers);
    }

    private void tryToConnectPeers() {
        try (DatagramSocket clientSocket = new DatagramSocket()) {
            for (NodeIdentity peer : peersToConnect) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String message = prependLengthToMessage("JOIN " + nodeIdentity.getIpAddress() + " " + nodeIdentity.getPort());
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Join message to '" + peer + "'");
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log.info("RECEIVE: " + responseMessage + " from '" + peer + "'");

                if (responseMessage.contains("JOINOK 0")) {
                    peers.add(peer);
                } else {
                    log.error("Error in connecting to the peer");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assignNewIdentity() {
        int port = 0;
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            port = serverSocket.getLocalPort();
        } catch (IOException e) {
        }
        this.nodeIdentity = NodeIdentity.of("localhost", port);
    }


    public void publish(String fileName) {
        for (NodeIdentity peer : peers) {
            try (DatagramSocket serverSocket = new DatagramSocket()) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String message = prependLengthToMessage("PUBLISH " + fileName + " " + nodeIdentity.getIpAddress() + " " + fileServerPort + " " + nodeIdentity.getPort());
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Publish message to '" + peer + "'");
                serverSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log.info("RECEIVE: " + responseMessage + " from '" + peer + "'");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void unregister() {
        //TODO
        for (NodeIdentity peer : peers) {
            try (DatagramSocket serverSocket = new DatagramSocket()) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String message = prependLengthToMessage("PUBLISH " + " " + nodeIdentity.getIpAddress() + " " + fileServerPort + " " + nodeIdentity.getPort());
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Publish message to '" + peer + "'");
                serverSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log.info("RECEIVE: " + responseMessage + " from '" + peer + "'");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
