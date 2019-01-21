package de.zell;

import de.zell.primitive.DistributedEngineBuilder;
import de.zell.primitive.DistributedEngineConfig;
import de.zell.primitive.DistributedEngineType;
import de.zell.primitive.api.client.DistributedEngine;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.utils.net.Address;

public class ClientMain {
  public static void main(String[] args) {

    final AtomixBuilder atomixBuilder = Atomix.builder();
    /// SETUP
    final Atomix node =
        atomixBuilder
            .withMemberId("client")
            .withAddress(new Address("localhost", 10_000))
            // for member detection
            .withMulticastEnabled()
            .build();

    node.start().join();

    final MultiRaftProtocol multiRaftProtocol =
        MultiRaftProtocol.builder("raft").withReadConsistency(ReadConsistency.LINEARIZABLE).build();

    final DistributedEngine distributedEngine =
        node.<DistributedEngineBuilder, DistributedEngineConfig, DistributedEngine>primitiveBuilder(
                "Engine", DistributedEngineType.instance())
            .withProtocol(multiRaftProtocol)
            .build();

    distributedEngine.async().newWorkflowInstance("fail");
  }
}
