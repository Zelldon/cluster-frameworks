package de.zell.primitive.api.client;

import io.atomix.primitive.SyncPrimitive;

public interface DistributedEngine extends SyncPrimitive {
  long append(byte[] bytes);

  @Override
  AsyncDistributedEngine async();
}
