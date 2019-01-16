package de.zell.logstream;

import io.atomix.primitive.operation.Command;

public interface DistributedLogstreamService {
  @Command
  void append(byte[] bytes);
}
