package de.zell;

import de.zell.logstream.DistributedLogstream;
import de.zell.logstream.DistributedLogstreamBuilder;
import de.zell.logstream.DistributedLogstreamConfig;
import de.zell.logstream.DistributedLogstreamType;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.utils.net.Address;
import org.apache.commons.lang3.RandomStringUtils;
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

    final int entries = 1024 * 256;
    final int entryLength = 128 * 1024;
    for (int i = 0; i < entries; i++) {
      final StringBuilder entryBuilder = new StringBuilder("entry-").append(i);
      int remainingLength = entryLength - entryBuilder.length();
      entryBuilder.append(RandomStringUtils.random(remainingLength));
      logstream.append(entryBuilder.toString().getBytes());
    }
  }
}
