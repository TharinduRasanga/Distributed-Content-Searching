package com.distributed.fs.config;

import com.distributed.fs.filesystem.FileManager;
import com.distributed.fs.node.Node;
import com.distributed.fs.node.NodeIdentity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Value("${bootstrap.host}")
    private String host;

    @Value("${bootstrap.port}")
    private String port;

    @Value("${server.port}")
    private String fileServerPort;

    @Bean
    public Node node(FileManager fileManager) {
        NodeIdentity bootstrapNode = NodeIdentity.of("localhost", Integer.parseInt(port));
        return new Node(bootstrapNode, fileManager, fileServerPort);
    }
}
