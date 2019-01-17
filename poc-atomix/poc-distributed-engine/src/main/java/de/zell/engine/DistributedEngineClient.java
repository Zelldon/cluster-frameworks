package de.zell.engine;

import io.atomix.primitive.event.Event;

public interface DistributedEngineClient {

  @Event
  void appended(long position);
}
