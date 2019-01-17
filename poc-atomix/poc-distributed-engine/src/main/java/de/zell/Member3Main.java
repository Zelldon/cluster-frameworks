package de.zell;

import static de.zell.Broker.ROOT_DIR;

import java.util.Arrays;
import java.util.List;

public class Member3Main {
  public static void main(String[] args) {
    final List<String> members = Arrays.asList(new String[] {"member1", "member2", "member3"});

    new Broker(ROOT_DIR, members.get(2), 26502, members).start();
  }
}
