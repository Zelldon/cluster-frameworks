package de.zell.logstream;

import io.atomix.primitive.SyncPrimitive;

public interface DistributedLogstream
   extends SyncPrimitive {
    long append(byte[] bytes);

    @Override
    AsyncDistributedLogstream async();
  }
