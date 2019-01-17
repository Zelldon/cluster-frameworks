package de.zell.engine;

import io.atomix.primitive.SyncPrimitive;

public interface DistributedEngine extends SyncPrimitive {
  long append(byte[] bytes);

  @Override
  AsyncDistributedEngine async();
}
