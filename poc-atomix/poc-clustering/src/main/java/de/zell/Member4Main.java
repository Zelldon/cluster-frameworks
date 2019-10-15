package de.zell;

import static de.zell.Primitive.ROOT_DIR;

import java.util.Arrays;
import java.util.List;

public class Member4Main {
  public static void main(String[] args) {
    final List<String> members =
        Arrays.asList("member1", "member2", "member3", "member4", "member5");

    new Primitive(ROOT_DIR, members.get(3), 26503, members).start();
  }
}
