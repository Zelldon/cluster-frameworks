package de.zell;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.Member;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.profile.Profile;

/** Hello world! */
public class App {

  public static void main(String[] args) throws Exception {
    new Thread(new AtomixNode("member1", 26500)).start();
    new Thread(new AtomixNode("member2", 26501)).start();
    new Thread(new AtomixNode("member3", 26502)).start();

    Thread.sleep(5_000);
    new Thread(new AtomixNode("laterJoinedMember", 26503)).start();
  }

  private static class AtomixNode implements Runnable {
    private final Random random = new Random();
    private final int TIME = 1000;

    private final String memberId;
    private final int port;
    private final int timeToLive;

    public AtomixNode(String memberId, int port) {
      this.memberId = memberId;
      this.port = port;
      timeToLive = random.nextInt() % 10_000 + 5_000;
    }

    @Override
    public void run() {
      final Atomix node = setupNode();
      printMembers(node);
      addMembershipListener(node);

      final ClusterCommunicationService communicationService = node.getCommunicationService();
      communicationService
          .subscribe("broadcast", (msg) ->
      {
        System.out.println("Node: " + memberId + " got message: " + msg);
        return CompletableFuture.completedFuture(msg);
      });

      int currentLifeTime = 0;
      while (currentLifeTime < timeToLive)
      {
        try {
          Thread.sleep(TIME);
          // broadcast
          communicationService
              .broadcast("broadcast", "This is the message");
          currentLifeTime += TIME;
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      node.stop().join();
    }

    private Atomix setupNode()
    {
      final AtomixBuilder atomixBuilder = Atomix.builder();

      final Atomix node =
          atomixBuilder
              .withMemberId(memberId)
              .withAddress(port)
              .withMulticastEnabled()
              .addProfile(Profile.dataGrid())
              .build();

      System.out.println("Start node: " + memberId);

      node.start().join();
      return node;
    }

    private void printMembers(Atomix node)
    {
      final Set<Member> members = node.getMembershipService().getMembers();

      final StringBuilder builder =
          new StringBuilder("Member with id: ").append(memberId).append(" knows from:");
      for (Member m : members) {
        builder.append("\n").append(m.id()).append(" member: ").append(m.toString());
      }
      System.out.println(builder.toString());
    }

    private void addMembershipListener(Atomix node)
    {
      node.getMembershipService()
          .addListener(
              clusterMembershipEvent -> {
                System.out.println(
                    "Member: "
                        + memberId
                        + " received cluster membership event"
                        + clusterMembershipEvent.toString());
              });
    }
  }
}
