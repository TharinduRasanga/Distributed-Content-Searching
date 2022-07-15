package com.distributed.fs.node;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString
public class NodeIdentity {

    private String ipAddress;
    private int port;

    private NodeIdentity(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String getName() {
        return "node-" + port;
    }

    public static NodeIdentity of(String ipAddress, int port) {
        return new NodeIdentity(ipAddress, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeIdentity that = (NodeIdentity) o;
        return port == that.port && Objects.equals(ipAddress, that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, port);
    }
}
