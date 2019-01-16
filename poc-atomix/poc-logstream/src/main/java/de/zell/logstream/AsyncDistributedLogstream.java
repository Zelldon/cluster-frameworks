package de.zell.logstream;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AsyncPrimitive;

public interface AsyncDistributedLogstream extends AsyncPrimitive {
  CompletableFuture<Long> append(byte[] bytes);

  @Override
  DistributedLogstream sync();
}
