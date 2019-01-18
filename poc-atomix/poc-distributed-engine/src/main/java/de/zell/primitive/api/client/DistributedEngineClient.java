package de.zell.primitive.api.client;

import io.atomix.primitive.event.Event;

public interface DistributedEngineClient {

  @Event
  void createdWorkflowInstance(long position);

  @Event
  void rejectWorkflowInstanceCreation(String reason);
}
