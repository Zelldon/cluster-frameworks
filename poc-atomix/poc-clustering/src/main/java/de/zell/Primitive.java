package de.zell;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.log.LogSession;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Primitive extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Primitive.class);

  public static final File ROOT_DIR = new File("/home/zell/atomix");

  private final String memberId;
  private final int port;
  private final List<String> allMemberList;
  private final File memberFolder;
  private final Set<String> members;

  boolean putValues = false;

  public Primitive(
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
            .withElectionTimeout(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 250)))
            .build();

    /// PARTITION GROUP

    final String logPartitionGroupName = "logGroup";
    final File logGroupFolder = new File(memberFolder, logPartitionGroupName);
    logGroupFolder.mkdir();

    final RaftPartitionGroup raftPartitionGroup =
        RaftPartitionGroup.builder("raft")
            .withNumPartitions(8)
            .withPartitionSize(3)
            .withMembers(allMemberList)
            .withDataDirectory(logGroupFolder)
            .withFlushOnCommit()
            .withElectionTimeout(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 250)))
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
    LOG.info("Part of {}", node.getPartitionService().getPartitionGroup("raft").getPartitionIds().toArray());

    final MultiRaftProtocol multiRaftProtocol =
        MultiRaftProtocol.builder().withReadConsistency(ReadConsistency.LINEARIZABLE).build();

    final Map<String, Integer> map =
        node.<String, Integer>mapBuilder("my-map").withProtocol(multiRaftProtocol).build();

    int count = 0;
    while (putValues)
    {
      try {
        LOG.warn("{}", map.put("foo" + (count % 32), count++));
        Thread.sleep(500L);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
