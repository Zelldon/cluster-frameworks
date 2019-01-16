package de.zell;

import de.zell.logstream.DistributedLogstream;
import de.zell.logstream.DistributedLogstreamBuilder;
import de.zell.logstream.DistributedLogstreamConfig;
import de.zell.logstream.DistributedLogstreamType;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.log.LogSession;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Primitive extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Primitive.class);

  public static final File ROOT_DIR = new File("/home/zell/atomix");

  public static final String CREATE_COMMAND = "CREATE";
  public static final String CREATED_EVENT = "CREATED";

  private final String memberId;
  private final int port;
  private final List<String> allMemberList;
  private final File memberFolder;
  private LogSession logSession;
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

    final String partitionGroupName = "partitionGroup";
    final File partitionGroupFolder = new File(memberFolder, partitionGroupName);
    partitionGroupFolder.mkdir();

    final RaftPartitionGroup raftPartitionGroup =
        RaftPartitionGroup.builder("raft")
            .withNumPartitions(3)
            .withPartitionSize(3)
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
        MultiRaftProtocol.builder().withReadConsistency(ReadConsistency.LINEARIZABLE).build();


    // build custom primitive
    final DistributedLogstream logstream = node.<DistributedLogstreamBuilder,
        DistributedLogstreamConfig,
        DistributedLogstream>primitiveBuilder("logstream", DistributedLogstreamType.instance())
        .withProtocol(multiRaftProtocol)
        .build();

    logstream.append(new UnsafeBuffer("foobar".getBytes()));
  }
}
