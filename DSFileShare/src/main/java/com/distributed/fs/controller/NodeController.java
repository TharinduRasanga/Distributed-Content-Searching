package com.distributed.fs.controller;

import com.distributed.fs.service.NodeService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController()
@RequestMapping("/node")
public class NodeController {
    private final NodeService nodeService;

    public NodeController(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @PostMapping("/publish")
    public void publishFile(@RequestBody String fileName) {
        nodeService.publishFile(fileName);
    }

    @GetMapping("/search/{name}")
    public List<String> search(@PathVariable String name) {
        return nodeService.search(name);
    }
}
