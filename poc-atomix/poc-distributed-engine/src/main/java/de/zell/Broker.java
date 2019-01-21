package de.zell;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.zell.primitive.DistributedEngineBuilder;
import de.zell.primitive.DistributedEngineConfig;
import de.zell.primitive.DistributedEngineType;
import de.zell.primitive.api.client.DistributedEngine;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.log.LogSession;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftCompactionConfig;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Broker.class);
  public static final File ROOT_DIR = new File("atomix");

  static {
    ROOT_DIR.mkdir();

    final Field freeDiskBuffer;
    try {
      freeDiskBuffer = RaftCompactionConfig.class.getDeclaredField("DEFAULT_FREE_DISK_BUFFER");
      freeDiskBuffer.setAccessible(true);

      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(freeDiskBuffer, freeDiskBuffer.getModifiers() & ~Modifier.FINAL);

      freeDiskBuffer.set(null, 0.43);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final String CREATE_COMMAND = "CREATE";
  public static final String CREATED_EVENT = "CREATED";

  private final String memberId;
  private final int port;
  private final List<String> allMemberList;
  private final File memberFolder;
  private LogSession logSession;
  private final Set<String> members;

  public Broker(
      final File rootFolder, final String memberId, int port, final List<String> memberList) {
    this.memberFolder = new File(rootFolder, memberId);
    this.memberFolder.mkdir();

    this.memberId = memberId;
    this.port = port;

    allMemberList = memberList;
    members = new HashSet<>();

    memberList.forEach(
        member -> {
          if (!member.equalsIgnoreCase(memberId)) {
            members.add(member);
          }
        });
  }

  public void run() {
    final AtomixBuilder atomixBuilder = Atomix.builder();

    /// SYSTEM PARTITION GROUP
    final String systemPartitionGroupName = "system";
    final File systemGroupFolder = new File(memberFolder, systemPartitionGroupName);
    systemGroupFolder.mkdir();

    final RaftPartitionGroup systemPartitionGroup =
        RaftPartitionGroup.builder(systemPartitionGroupName)
            .withNumPartitions(1)
            .withMembers(allMemberList)
            .withDataDirectory(systemGroupFolder)
            .withFlushOnCommit()
            .build();

    /// PARTITION GROUP

    final String partitionGroupName = "raft";
    final File partitionGroupFolder = new File(memberFolder, partitionGroupName);
    partitionGroupFolder.mkdir();

    final RaftPartitionGroup raftPartitionGroup =
        RaftPartitionGroup.builder(partitionGroupName)
            .withNumPartitions(1)
            .withPartitionSize(1)
            .withMembers(allMemberList)
            .withDataDirectory(partitionGroupFolder)
            .withFlushOnCommit()
            .build();

    /// SETUP
    final Atomix node =
        atomixBuilder
            .withMemberId(memberId)
            .withAddress(new Address("localhost", port))
            // for member detection
            .withMulticastEnabled()
            // must have - to coordinate leader election
            .withManagementGroup(systemPartitionGroup)
            .addPartitionGroup(raftPartitionGroup)
            .build();

    node.start().join();
    LOG.info("Member {} started.", memberId);

    final MultiRaftProtocol multiRaftProtocol =
        MultiRaftProtocol.builder(partitionGroupName)
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .build();

    LOG.info("Build Engine primitive.");
    // build custom primitive
    node.<DistributedEngineBuilder, DistributedEngineConfig, DistributedEngine>primitiveBuilder(
            "Engine", DistributedEngineType.instance())
        .withProtocol(multiRaftProtocol)
        .build();

    LOG.info("Engine primitive build.");
  }
}
