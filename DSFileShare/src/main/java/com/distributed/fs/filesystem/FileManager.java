package com.distributed.fs.filesystem;

import com.distributed.fs.dto.SearchResult;
import com.distributed.fs.node.NodeIdentity;
import org.springframework.stereotype.Component;

import java.util.*;
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

    public Set<String> getLocalMatching(String fileName) {
        Set<String> queriedFileList = new HashSet<>();

        for (String file : localFileNames) {
            if (file.toLowerCase().matches(".*\\b" + fileName.toLowerCase() + "\\b.*")) {
                queriedFileList.add(file);
            }
        }
        return queriedFileList;
    }

    public Set<SearchResult> getPeerMatching(String fileName) {
        Set<SearchResult> result = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : fileTable.entrySet()) {
            String key = entry.getKey();
            if (key.toLowerCase().matches(".*\\b" + fileName.toLowerCase() + "\\b.*")) {
                for (String s : entry.getValue()) {
                    SearchResult searchResult = new SearchResult();
                    searchResult.setFileName(key);
                    searchResult.setLocation(s);
                    result.add(searchResult);
                }
            }
        }
        return result;
    }
}
