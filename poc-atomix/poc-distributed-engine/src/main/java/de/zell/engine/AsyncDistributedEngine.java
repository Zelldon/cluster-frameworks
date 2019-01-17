package de.zell.engine;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AsyncPrimitive;

public interface AsyncDistributedEngine extends AsyncPrimitive {
  CompletableFuture<Long> append(byte[] bytes);

  @Override
  DistributedEngine sync();
}
