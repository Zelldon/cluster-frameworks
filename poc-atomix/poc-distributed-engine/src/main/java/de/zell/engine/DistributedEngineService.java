package de.zell.engine;

import io.atomix.primitive.operation.Command;

public interface DistributedEngineService {
  @Command
  void append(byte[] bytes);
}
