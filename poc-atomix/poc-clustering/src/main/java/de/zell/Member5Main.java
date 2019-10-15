package de.zell;

import static de.zell.Primitive.ROOT_DIR;

import java.util.Arrays;
import java.util.List;

public class Member5Main {
  public static void main(String[] args) {
    final List<String> members =
        Arrays.asList("member1", "member2", "member3", "member4", "member5");

    Primitive primitive = new Primitive(ROOT_DIR, members.get(4), 26504, members);
    primitive.putValues = true;
    primitive.start();
  }
}
