package de.zell.primitive.api.server;

import io.atomix.primitive.operation.Command;

public interface DistributedEngineService {
  @Command
  void newWorkflowInstance(String workflowId);

  @Command
  void executeStartEvent(long workflowId);

  @Command
  void executeEndEvent(long workflowId);
}
