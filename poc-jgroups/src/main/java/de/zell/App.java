package de.zell;

import java.util.Random;

import org.jgroups.*;

/** Hello world! */
public class App {
  public static void main(String[] args) throws Exception {
    System.setProperty("java.net.preferIPv4Stack", "true");
    new Thread(new Node("mem1", 26500)).start();
    new Thread(new Node("mem2", 26501)).start();
    new Thread(new Node("mem3", 26502)).start();

    Thread.sleep(2000);
    new Thread(new Node("mem4", 26503)).start();
  }

  private static final class Node extends ReceiverAdapter implements Runnable {
    private final Random random = new Random();
    private final int TIME = 1000;

    private String nodeId;
    private final int port;
    private final long timeToLive;

    public Node(String nodeId, int port) {
      this.port = port;

      timeToLive = random.nextInt() % 10_000 + 15_000;
    }

    @Override
    public void run() {
      try (final JChannel channel = new JChannel()) {
        channel.setReceiver(this);
        channel.setName(nodeId);
        channel.connect("zeebe-cluster");

        nodeId = channel.getAddressAsString();

        printMemberList(channel);

        int currentLifeTime = 0;
        while (currentLifeTime < timeToLive) {
          try {
            Thread.sleep(TIME);

            // broadcast
            channel.send(new Message(null, "this is the message"));

            currentLifeTime += TIME;
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        System.out.println("Node " + nodeId + " leaves cluster");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private void printMemberList(JChannel channel) {
      final StringBuilder builder =
          new StringBuilder("Member ").append(channel.address()).append(" knows ");
      for (Address a : channel.getView().getMembers()) {
        builder.append("\n").append(a);
      }
      System.out.println(builder.toString());
    }

    @Override
    public void receive(Message msg) {
      System.out.println("Node " + nodeId + " receives " + msg.toString());
    }

    @Override
    public void viewAccepted(View view) {
      System.out.println("Node " + nodeId + " got new view" + view.toString());
    }

    @Override
    public void suspect(Address mbr) {
      System.out.println("Node " + nodeId + " suspect member " + mbr);
    }
  }
}
