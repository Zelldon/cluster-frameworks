package de.zell.engine;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.PrimitiveConfig;

public class DistributedEngineConfig extends PrimitiveConfig<DistributedEngineConfig> {

  @Override
  public PrimitiveType getType() {
    return DistributedEngineType.instance();
  }
}
