package de.zell.logstream;

import io.atomix.primitive.event.Event;

public interface DistributedLogstreamClient {

  @Event
  void appended(long position);
}
