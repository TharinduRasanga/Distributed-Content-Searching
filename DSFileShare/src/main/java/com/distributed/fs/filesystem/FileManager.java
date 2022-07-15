package com.distributed.fs.filesystem;

import com.distributed.fs.node.NodeIdentity;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FileManager {

    private final Set<String> localFileNames = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<String>> fileTable = new ConcurrentHashMap<>();

    public void addLocalFile(String fileName) {
        localFileNames.add(fileName);
    }

    public void removeLocalFile(String fileName) {
        localFileNames.remove(fileName);
    }

    public boolean containsLocally(String fileName) {
        return localFileNames.contains(fileName);
    }

    public void addToFileTable(String fileName, String location) {
        if (fileTable.containsKey(fileName)) {
            fileTable.get(fileName).add(location);
        } else {
            HashSet<String> set = new HashSet<>();
            set.add(location);
            fileTable.put(fileName, set);
        }
    }

    public void removeFromFileTable(NodeIdentity node) {
        // localhost:fileserverport:udpport
    }
}
