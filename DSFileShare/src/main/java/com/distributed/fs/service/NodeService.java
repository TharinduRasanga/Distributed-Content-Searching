package com.distributed.fs.service;

import com.distributed.fs.filesystem.FileManager;
import com.distributed.fs.node.Node;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class NodeService {

    private final Node node;
    private final FileManager fileManager;

    public NodeService(Node node, FileManager fileManager) {
        this.node = node;
        this.fileManager = fileManager;
    }

    public Map<String, Set<String>> search(String name) {
        return node.search(name);
    }

    public void publishFile(String fileName) {
        fileManager.addLocalFile(fileName);
        node.publish(fileName);
    }

    public void unregister() {
        node.unregister();
    }
}
