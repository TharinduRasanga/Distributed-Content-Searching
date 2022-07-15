package com.distributed.fs.service;

import com.distributed.fs.filesystem.FileManager;
import com.distributed.fs.node.Node;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NodeService {

    private final Node node;
    private final FileManager fileManager;

    public NodeService(Node node, FileManager fileManager) {
        this.node = node;
        this.fileManager = fileManager;
    }

    public List<String> search(String name) {
        return null;
    }

    public void publishFile(String fileName) {
        fileManager.addLocalFile(fileName);
        node.publish(fileName);
    }
}
