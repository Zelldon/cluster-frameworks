package de.zell.primitive.api.client;

import io.atomix.primitive.SyncPrimitive;

public interface DistributedEngine extends SyncPrimitive {
  long newWorkflowInstance(String workflowId);

  @Override
  AsyncDistributedEngine async();
}
