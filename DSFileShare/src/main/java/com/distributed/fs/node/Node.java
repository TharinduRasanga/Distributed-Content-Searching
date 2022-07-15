package com.distributed.fs.node;

import com.distributed.fs.Constants;
import com.distributed.fs.dto.SearchResult;
import com.distributed.fs.filesystem.FileManager;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
                log.info("income {}", incomingMessage);

                //incomingMessage = 0047 SER 129.82.62.142 5070 "Lord of the rings" 3

                String[] response = splitIncomingMessage(incomingMessage);
                byte[] sendData = null;

                InetAddress responseAddress = incoming.getAddress();
                int responsePort = incoming.getPort();

                if (response.length >= 6 && Constants.SER.equals(response[1])) {
                    String host = response[2];
                    String searcherPort = response[3];
//                    if (NodeIdentity.of(host, Integer.parseInt(searcherPort)).equals(nodeIdentity)) {
//                        continue;
//                    }
                    InetAddress searcherAddress = InetAddress.getByName(host);
                    String searchFilename = getResourceNameFromSearchQuery(response[4]);
                    int currentHopsCount = Integer.parseInt(response[5]);
                    if (currentHopsCount == 0) {
                        log.info("DROP: Maximum hops reached hence dropping '" + incomingMessage + "'");
                        continue;
                    }
                    Set<String> localMatching = fileManager.getLocalMatching(searchFilename);
                    Set<SearchResult> peerMatching = fileManager.getPeerMatching(searchFilename);

                    int totalCount = localMatching.size() + peerMatching.size();

                    // Check whether file exist locally
                    if (localMatching.size() > 0) {
                        StringBuilder fileNames = new StringBuilder();
                        for (String s : localMatching) {
                            fileNames.append(s).append(" ");
                        }
                        byte[] localData = prependLengthToMessage(
                                "SEROK " + localMatching.size() + " " + nodeIdentity.getIpAddress()
                                        + " " + fileServerPort + " " + fileNames.toString().trim()
                        ).getBytes();
                        DatagramPacket sendPacket = new DatagramPacket(localData, localData.length, searcherAddress,
                                Integer.parseInt(searcherPort));
                        serverSocket.send(sendPacket);
                    }

                    // check whether file is in file table
                    if (peerMatching.size() > 0) {
                        Map<String, Set<String>> fileNamesByIp = peerMatching.stream()
                                .collect(Collectors.groupingBy(
                                        SearchResult::getLocation,
                                        Collectors.mapping(SearchResult::getFileName, Collectors.toSet())
                                ));
                        for (Map.Entry<String, Set<String>> entry : fileNamesByIp.entrySet()) {
                            String[] location = entry.getKey().split(":");
                            Set<String> fileNames = entry.getValue();
                            StringBuilder names = new StringBuilder();
                            for (String s : fileNames) {
                                names.append(s).append(" ");
                            }
                            byte[] localData = prependLengthToMessage(
                                    "SEROK " + localMatching.size() + " " + location[0]
                                            + " " + location[1] + " " + names.toString().trim()
                            ).getBytes();
                            DatagramPacket sendPacket = new DatagramPacket(localData, localData.length, searcherAddress,
                                    Integer.parseInt(searcherPort));
                            serverSocket.send(sendPacket);
                        }
                    }

                    // Flood the query to all peers
                    response[5] = String.valueOf(currentHopsCount - 1);
                    String updatedSearchQuery = String.join(" ", response);

                    for (NodeIdentity peer : peers) {
                        try (DatagramSocket peerSocket = new DatagramSocket()) {
                            byte[] floodData = updatedSearchQuery.getBytes();
                            DatagramPacket sendPacket = new DatagramPacket(floodData, floodData.length,
                                    InetAddress.getByName(peer.getIpAddress()), peer.getPort());
                            log.info("FLOOD: Search query to '" + updatedSearchQuery + "'");
                            serverSocket.send(sendPacket);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                } else if (response.length >= 4 && JOIN.equals(response[1])) {
                    sendData = processJoinRequest(incomingMessage, response, responseAddress, responsePort);
                } else if (response.length >= 4 && LEAVE.equals(response[1])) {
                    sendData = processLeaveRequest(incomingMessage, response, responseAddress, responsePort);
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
        NodeIdentity node = NodeIdentity.of(
                responseAddress.getHostAddress().equals("127.0.0.1") ? "localhost" : responseAddress.getHostAddress(),
                Integer.parseInt(response[3]));
        peers.remove(node);
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
            String message = prependLengthToMessage("REG " + nodeIdentity.getIpAddress() + " " + nodeIdentity.getPort() + " " + nodeIdentity.getName());
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
        int port = getFreePort();
        this.nodeIdentity = NodeIdentity.of("localhost", port);
    }


    public void publish(String fileName) {
        for (NodeIdentity peer : peers) {
            try (DatagramSocket serverSocket = new DatagramSocket()) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String message = prependLengthToMessage("PUBLISH \"" + fileName + "\" " + nodeIdentity.getIpAddress() + " " + fileServerPort + " " + nodeIdentity.getPort());
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Publish message to '" + peer + "'");
                serverSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.setSoTimeout(2000);
                try {
                    serverSocket.receive(receivePacket);
                    String responseMessage = new String(receivePacket.getData()).trim();
                    log.info("RECEIVE: " + responseMessage + " from '" + peer + "'");
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void unregister() {
        unRegisterFromBootstrapServer();

        for (NodeIdentity peer : peers) {
            try (DatagramSocket serverSocket = new DatagramSocket()) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String message = prependLengthToMessage("LEAVE " + nodeIdentity.getIpAddress() + " " + nodeIdentity.getPort());
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Leave message to '" + peer + "'");
                serverSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log.info("RECEIVE: " + responseMessage + " from '" + peer + "'");
                peers = new HashSet<>();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @PreDestroy
    public void unRegisterFromBootstrapServer() {
        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIdentity.getIpAddress());
            DatagramSocket clientSocket = null;
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = prependLengthToMessage("UNREG " + nodeIdentity.getIpAddress() + " " + nodeIdentity.getPort() + " " + nodeIdentity.getName());
            byte[] sendData = message.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, bootstrapHost, 55555);
            log.info("SEND: Bootstrap server unregister message '" + message + "'");
            clientSocket.send(sendPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Set<String>> search(String name) {
        Set<SearchResult> results = new HashSet<>();
        for (NodeIdentity peer : peers) {
            int searchPort = getFreePort();
            try (DatagramSocket serverSocket = new DatagramSocket(searchPort)) {
                InetAddress address = InetAddress.getByName(peer.getIpAddress());
                byte[] receiveData = new byte[1024];
                String searchQuery = "SER " + nodeIdentity.getIpAddress() + " " + searchPort + " " + name + " " + 5;
                String message = prependLengthToMessage(searchQuery);
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log.info("SEND: Search query '" + message + "'");
                serverSocket.send(sendPacket);

                long start = System.currentTimeMillis();
                long end = System.currentTimeMillis();
                int threshold = 3*1000;
                serverSocket.setSoTimeout(2000);
                while (true) {
                    end = System.currentTimeMillis();
                    if ((end - start) > threshold) {
                        break;
                    }
                    byte[] buffer = new byte[65536];
                    DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                    try {
                        serverSocket.receive(incoming);
                    } catch (SocketTimeoutException e) {
                        continue;
                    }

                    byte[] data = incoming.getData();
                    String incomingMessage = new String(data, 0, incoming.getLength());

                    String[] response = splitIncomingMessage(incomingMessage);

                    log.info("RECEIVE: " + incomingMessage + " from '" + peer + "'");

                    if (SEROK.equals(response[1])) {
                        int noOfFiles = Integer.parseInt(response[2]);
                        String host = response[3];
                        String port = response[4];
                        for (int i = 5; i < 5 + noOfFiles; i++) {
                            SearchResult searchResult = new SearchResult();
                            searchResult.setLocation(host + ":" + port);
                            searchResult.setFileName(response[i].replace("\"", ""));
                            results.add(searchResult);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Set<String> localMatching = fileManager.getLocalMatching(name);
        if (!localMatching.isEmpty()) {
            for (String s : localMatching) {
                SearchResult searchResult = new SearchResult();
                searchResult.setLocation(nodeIdentity.getIpAddress() + ":" + fileServerPort);
                searchResult.setFileName(s.replace("\"", ""));
                results.add(searchResult);
            }
        }
        return results.stream()
                .collect(
                        Collectors.groupingBy(
                                SearchResult::getFileName,
                                Collectors.mapping(
                                        SearchResult::getLocation,
                                        Collectors.toSet()
                                )
                        )
                );
    }
}
