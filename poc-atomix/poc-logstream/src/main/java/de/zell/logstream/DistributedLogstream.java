package de.zell.logstream;

import io.atomix.primitive.SyncPrimitive;
import org.agrona.DirectBuffer;

public interface DistributedLogstream
   extends SyncPrimitive {
    long append(DirectBuffer bytes);

    @Override
    AsyncDistributedLogstream async();
  }
