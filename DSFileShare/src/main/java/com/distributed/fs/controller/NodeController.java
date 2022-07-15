package com.distributed.fs.controller;

import com.distributed.fs.service.NodeService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public Map<String, Set<String>> search(@PathVariable String name) {
        return nodeService.search(name);
    }

    @PostMapping("/unregister")
    public void unregister() {
        nodeService.unregister();
    }
}
