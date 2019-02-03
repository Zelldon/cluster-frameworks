package de.zell;

import com.google.common.io.Files;
import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.AtomixClusterBuilder;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.protocol.SwimMembershipProtocol;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtomixTest {

  private static final Logger LOG = LoggerFactory.getLogger(AtomixTest.class);

  private static AtomixCluster createAtomixNode(int nodeId, int port, int ... nodes) throws Exception
  {
    final List<Node> nodeList = new ArrayList();
    for (int node : nodes)
    {
      nodeList.add(Node.builder().withAddress(Address.from(node)).build());
    }

    final NodeDiscoveryProvider discoveryProvider = BootstrapDiscoveryProvider.builder().withNodes(nodeList).build();
    final Properties properties = new Properties();
    properties.setProperty("port", "" + port);

    final String clusterName = "zeebe-cluster";
    final String localMemberId = Integer.toString(nodeId);
    LOG.error("Setup atomix node in cluster {}", clusterName);

    final AtomixClusterBuilder atomixBuilder =
        AtomixCluster.builder()
            .withClusterId(clusterName)
            .withMembershipProtocol(
                SwimMembershipProtocol.builder()
                    .withFailureTimeout(Duration.ofMillis(2_000))
                    .build())
            .withMemberId(localMemberId)
            .withProperties(properties)
            .withAddress(Address.from(port))
            .withMembershipProvider(discoveryProvider);

    final AtomixCluster atomix = atomixBuilder.build();

    // only logging purpose for now
    atomix
        .getMembershipService()
        .addListener(mEvent -> LOG.info("Member {} receives {}", localMemberId, mEvent.toString()));

    return atomix;
  }

  public static void main(String[] args) throws Exception {
    // given
    final AtomixCluster  atomixNode = createAtomixNode(0, 26501);
    atomixNode.start().get();
    createAtomixNode(1, 26502, 26501).start().get();
    createAtomixNode(2, 26503, 26502).start().get();

    // when
    atomixNode.stop().get();
    LOG.error("Wait 'till member is removed");
    Thread.sleep(15_000);

    // then
    LOG.error("Restart node...");
    createAtomixNode(0, 26501).start().get();
  }
}
