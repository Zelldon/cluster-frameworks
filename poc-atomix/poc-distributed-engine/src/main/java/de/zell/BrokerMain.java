package de.zell;

import static de.zell.Broker.ROOT_DIR;

import java.util.Arrays;
import java.util.List;

public class BrokerMain {
  public static void main(String[] args) {
    final List<String> members = Arrays.asList(new String[] {"member1"});

    new Broker(ROOT_DIR, members.get(0), 26500, members).start();
  }
}
