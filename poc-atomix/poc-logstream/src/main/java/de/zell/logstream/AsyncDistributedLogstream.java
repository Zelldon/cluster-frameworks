package de.zell.logstream;

import io.atomix.primitive.AsyncPrimitive;
import java.util.concurrent.CompletableFuture;
import org.agrona.DirectBuffer;

public interface AsyncDistributedLogstream extends AsyncPrimitive {
  CompletableFuture<Long> append(DirectBuffer bytes);

  @Override
  DistributedLogstream sync();
}
