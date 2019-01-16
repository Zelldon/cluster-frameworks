package de.zell.logstream;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

public class DistributedLogstreamConfig extends PrimitiveConfig<DistributedLogstreamConfig> {

  @Override
  public PrimitiveType getType() {
    return DistributedLogstreamType.instance();
  }
}
