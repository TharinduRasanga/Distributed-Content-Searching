package com.distributed.fs.service;

import com.distributed.fs.node.Node;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NodeService {

    private final Node node;

    public NodeService(Node node) {
        this.node = node;
    }

    public List<String> search(String name) {
        return null;
    }
}
