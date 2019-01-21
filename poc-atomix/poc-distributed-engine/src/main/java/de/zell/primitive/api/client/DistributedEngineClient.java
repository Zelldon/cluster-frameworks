package de.zell.primitive.api.client;

import io.atomix.primitive.event.Event;

public interface DistributedEngineClient {

  @Event
  void createdWorkflowInstance(long position);

  @Event
  void rejectWorkflowInstanceCreation(String reason);

  @Event
  void rejectActivityExecution(String reason);

  @Event
  void startEventExecuted(long workflowId);

  @Event
  void endEventExecuted(long workflowId);
}
