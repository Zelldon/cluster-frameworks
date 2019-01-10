package de.zell;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.Member;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.Record;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Replication;
import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Broker extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Broker.class);

  public static final File ROOT_DIR = new File("/home/zell/atomix");

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

    /// LOG PARTITION GROUP

    final String logPartitionGroupName = "logGroup";
    final File logGroupFolder = new File(memberFolder, logPartitionGroupName);
    systemGroupFolder.mkdir();

    final LogPartitionGroup logGroup =
        LogPartitionGroup.builder(logPartitionGroupName)
            .withDataDirectory(logGroupFolder)
            .withMemberGroupStrategy(MemberGroupStrategy.NODE_AWARE)
            .withFlushOnCommit()
            .withNumPartitions(3)
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
            .addPartitionGroup(logGroup)
            //            .addPartitionGroup(
            //                RaftPartitionGroup.builder("raft")
            //                    .withNumPartitions(3)
            //                    .withMembers(allMemberList)
            //                    .withDataDirectory(logGroupFolder)
            //                    .withFlushOnCommit()
            //                    .build())
            .build();

    node.start().join();

    LOG.info("Member {} started.", memberId);

    printMembers(node);
    node.getMembershipService().addListener(this::listenMembership);

    final PartitionService partitionService = node.getPartitionService();
    final PartitionGroup partitionGroup = partitionService.getPartitionGroup("logGroup");
    if (partitionGroup != null) {
      partitionGroup
          .getPartitionIds()
          .forEach(partitionId -> LOG.info("Started partition {}", partitionId.toString()));
    }

    final DistributedLogProtocol logProtocol =
        DistributedLogProtocol.builder(logPartitionGroupName)
            .withConsistency(Consistency.SEQUENTIAL)
            .withReplication(Replication.ASYNCHRONOUS)
            .build();


    final DistributedLog<String> distributedLog =
        node.<String>logBuilder().withProtocol(logProtocol).build();

    distributedLog.consume(this::consumeRecord);

    //
    //    final DistributedLogProtocol logProtocol =
    //        DistributedLogProtocol.builder("logGroup")
    //            .withConsistency(Consistency.SEQUENTIAL)
    //            .withReplication(Replication.ASYNCHRONOUS)
    //            .build();
    //
    //    logProtocol
    //        .newClient(partitionService)
    //        .getPartitions()
    //        .forEach(session -> session.consumer().consume(this::consumeRecord));
  }

  private void consumeRecord(Record<String> record) {
    if (record == null) {
      LOG.warn("Record is null!");
      return;
    }

    LOG.info("Consume record {}", record.toString());

    final String value = new String(record.value());
    if (value.equalsIgnoreCase(CREATE_COMMAND)) {
      //      logSession.producer().append(CREATED_EVENT.getBytes());
    }
  }

  private void consumeLogRecord(LogRecord record) {
    if (record == null) {
      LOG.warn("Record is null!");
      return;
    }

    LOG.info("Consume record {}", record.toString());

    final String value = new String(record.value());
    if (value.equalsIgnoreCase(CREATE_COMMAND)) {
      //      logSession.producer().append(CREATED_EVENT.getBytes());
    }
  }

  private void printMembers(Atomix node) {
    final Set<Member> members = node.getMembershipService().getMembers();

    final StringBuilder builder =
        new StringBuilder("Member with id: ").append(memberId).append(" knows from:");
    for (Member m : members) {
      builder.append("\n").append(m.id()).append(" member: ").append(m.toString());
    }
    LOG.info(builder.toString());
  }

  private void listenMembership(ClusterMembershipEvent membershipEvent) {
    LOG.info(
        "Member: {} received cluster membership event {}.", memberId, membershipEvent.toString());
  }
}
