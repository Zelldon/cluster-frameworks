package de.zell;

import de.zell.logstream.DistributedLogstream;
import de.zell.logstream.DistributedLogstreamBuilder;
import de.zell.logstream.DistributedLogstreamConfig;
import de.zell.logstream.DistributedLogstreamType;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Primitive extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Primitive.class);
  public static final File ROOT_DIR = new File("atomix");

  static {
    ROOT_DIR.mkdir();
  }

  private final String memberId;
  private final int port;
  private final List<String> allMemberList;
  private final File memberFolder;
  private final Set<String> members;

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
            .build();

    /// PARTITION GROUP

    final String partitionGroupName = "raft";
    final File partitionGroupFolder = new File(memberFolder, partitionGroupName);
    partitionGroupFolder.mkdir();

    final RaftPartitionGroup raftPartitionGroup =
        RaftPartitionGroup.builder(partitionGroupName)
            .withNumPartitions(allMemberList.size())
            .withPartitionSize(allMemberList.size())
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

    LOG.info("Build logstream primitive.");
    // build custom primitive
    node.<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
            primitiveBuilder("logstream", DistributedLogstreamType.instance())
        .withProtocol(multiRaftProtocol)
        .build();

    LOG.info("Logstream primitive build.");

    LOG.info("Create leader election primitive.");

    final MultiRaftProtocol multiRaftOnSystem =
        MultiRaftProtocol.builder(systemPartitionGroupName)
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .build();

    raftPartitionGroup
        .getPartitions()
        .forEach(
            partition -> {
              final String partitionString =
                  new StringBuilder()
                      .append(partition.id().group())
                      .append("-")
                      .append("partition")
                      .append("-")
                      .append(partition.id().id())
                      .toString();
              final LeaderElection<String> election =
                  node.<String>leaderElectionBuilder(partitionString)
                      .withProtocol(multiRaftOnSystem)
                      .build();

              LOG.info("Created leader election primitive {}.", partitionString);
              election.addListener(this::onLeadershipEvent);

              election.run(memberId);
            });
  }

  private void onLeadershipEvent(LeadershipEvent<String> leadershipEvent) {
    final Leader<String> newLeader = leadershipEvent.newLeadership().leader();
    final Leadership<String> oldLeadership = leadershipEvent.oldLeadership();
    final Leader<String> oldLeader = oldLeadership.leader();

    final boolean leaderHasNotChanged = newLeader.equals(oldLeader);
    if (leaderHasNotChanged) {
      final boolean wasMemberBefore = oldLeadership.candidates().contains(memberId);
      if (wasMemberBefore) {
        return;
      }
    }

    final String leaderId = newLeader.id();
    if (leaderId.equalsIgnoreCase(memberId)) {
      LOG.error("I'm {} and take the lead for partition {}", memberId, leadershipEvent.topic());
    } else {
      LOG.info("I'm {} and will simply follow partition {}", memberId, leadershipEvent.topic());
    }
  }
}
