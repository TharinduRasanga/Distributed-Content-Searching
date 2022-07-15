package com.distributed.fs;

import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    public static String prependLengthToMessage(String message) {
        return String.format("%04d", message.length() + 5) + " " + message;
    }

    public static boolean isIP(String hostAddress) {
        return hostAddress.split("[.]").length == 4;
    }

    public static String[] splitIncomingMessage(String incomingMessage) {
        List<String> list = new ArrayList<>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(
                incomingMessage.replace("[", "").replace("]", "")
        );
        while (m.find()) {
            list.add(m.group(1));
        }
        return list.toArray(new String[list.size()]);
    }

    public static String createRandomContentToFile() {
        Random random = new Random();
        int randContent = random.nextInt(100000000);
        String str = String.valueOf(randContent);
        return str.repeat(1250000);
    }

    public static String getResourceNameFromSearchQuery(String query) {
        return query.trim().replaceAll("\"", "");
    }

    public static int getFreePort() {
        int port = 0;
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            port = serverSocket.getLocalPort();
        } catch (IOException e) {
        }
        return port;
    }

}
