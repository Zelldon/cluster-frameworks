package de.zell;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Files;

public class BrokerMain {

  public static void main(String[] args) {
    final List<String> members = Arrays.asList(new String[] {"member1", "member2", "member3"});

    final File tempDir = Files.createTempDir();
    new Broker(tempDir, members.get(0), 26500, members).start();
    new Broker(tempDir, members.get(1), 26501, members).start();
    new Broker(tempDir, members.get(2), 26502, members).start();
  }
}
