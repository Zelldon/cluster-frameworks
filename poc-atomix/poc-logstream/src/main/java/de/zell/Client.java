package de.zell;

import de.zell.logstream.DistributedLogstream;
import de.zell.logstream.DistributedLogstreamBuilder;
import de.zell.logstream.DistributedLogstreamConfig;
import de.zell.logstream.DistributedLogstreamType;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private final String memberId;
  private final int port;

  public Client(String memberId, int port) {
    this.memberId = memberId;
    this.port = port;
  }

  public void run() {
    final AtomixBuilder atomixBuilder = Atomix.builder();

    /// SETUP
    final Atomix node =
        atomixBuilder
            .withMemberId(memberId)
            .withAddress(new Address("localhost", port))
            // for member detection
            .withMulticastEnabled()
            .build();

    node.start().join();
    LOG.info("Member {} started.", memberId);

    final MultiRaftProtocol multiRaftProtocol =
        MultiRaftProtocol.builder("raft").withReadConsistency(ReadConsistency.LINEARIZABLE).build();

    LOG.info("Build logstream primitive.");
    // build custom primitive
    final DistributedLogstream logstream =
        node.<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
                primitiveBuilder("logstream", DistributedLogstreamType.instance())
            .withProtocol(multiRaftProtocol)
            .build();

    LOG.info("Logstream primitive build.");
    logstream.append("foobar".getBytes());
  }
}
