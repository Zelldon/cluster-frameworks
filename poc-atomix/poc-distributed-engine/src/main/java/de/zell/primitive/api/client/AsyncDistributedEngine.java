package de.zell.primitive.api.client;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AsyncPrimitive;

public interface AsyncDistributedEngine extends AsyncPrimitive {
  CompletableFuture<Long> newWorkflowInstance(String workflowId);

  @Override
  DistributedEngine sync();
}
