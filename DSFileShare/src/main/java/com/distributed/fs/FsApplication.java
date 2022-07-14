package com.distributed.fs;

import com.distributed.fs.node.Node;
import com.distributed.fs.node.NodeIdentity;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FsApplication {

	public static void main(String[] args) {
//		NodeIdentity bootstrapNode = NodeIdentity.of("localhost", 5555);
//		Node node1 = new Node(bootstrapNode);
//		Node node2 = new Node(bootstrapNode);
//		node1.connectToBootstrapServer();
//		node2.connectToBootstrapServer();
		SpringApplication.run(FsApplication.class, args);
	}

}
