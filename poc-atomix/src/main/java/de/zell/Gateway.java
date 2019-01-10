package de.zell;

import java.util.Scanner;

import io.atomix.core.Atomix;
import io.atomix.core.log.DistributedLog;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Replication;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gateway {
  private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  public static void main(String[] args) {

    //    final DistributedLogProtocol logProtocol =
    //        DistributedLogProtocol.builder("logGroup")
    //            .withConsistency(Consistency.SEQUENTIAL)
    //            .withReplication(Replication.ASYNCHRONOUS)
    //            .build();

    final Atomix atomix =
        Atomix.builder()
            .withMemberId("client")
            .withAddress(Address.from(10001))
            //            .withClusterId("test")
            .withMulticastEnabled()
            .build();

    atomix.start().join();

    LOG.info("Connected to cluster.");

    final DistributedLogProtocol logProtocol =
        DistributedLogProtocol.builder("logGroup")
            .withConsistency(Consistency.SEQUENTIAL)
            .withReplication(Replication.ASYNCHRONOUS)
            .build();

    final DistributedLog<String> distributedLog =
        atomix.<String>logBuilder().withProtocol(logProtocol).build();

    //    final LogClient logClient = logProtocol.newClient(atomix.getPartitionService());
    final Scanner scanner = new Scanner(System.in);
    while (scanner.hasNext()) {
      //      final String correlationKey = scanner.next();
      //      final LogSession partition = logClient.getPartition(correlationKey);
      //      LOG.info("Partition {}", partition.partitionId().toString());

      //      if (scanner.hasNext()) {
      final String value = scanner.next();
      //        partition.producer().append(value.getBytes());

      distributedLog.produce(value);
      LOG.info("produced on log {}", value);

      //        LOG.info("Produced on partition {}", partition.partitionId().toString());
      //      }
    }
  }
}
