package de.zell;

import static de.zell.Broker.ROOT_DIR;

import java.util.Arrays;
import java.util.List;

public class Member2Main {
  public static void main(String[] args) {
    final List<String> members =
        Arrays.asList(new String[] {"member1", "member2", "member3", "member4", "member5"});

    new Broker(ROOT_DIR, members.get(1), 26501, members).start();
  }
}
