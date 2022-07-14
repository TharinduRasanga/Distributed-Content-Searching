package com.distributed.fs;

import java.util.ArrayList;
import java.util.List;
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
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(incomingMessage);
        while (m.find()) {
            list.add(m.group(1));
        }
        return list.toArray(new String[list.size()]);
    }

}
