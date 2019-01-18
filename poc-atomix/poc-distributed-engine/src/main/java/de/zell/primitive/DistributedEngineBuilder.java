package de.zell.primitive;

import de.zell.primitive.api.client.DistributedEngine;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;

public abstract class DistributedEngineBuilder
    extends PrimitiveBuilder<DistributedEngineBuilder, DistributedEngineConfig, DistributedEngine>
    implements ProxyCompatibleBuilder<DistributedEngineBuilder> {

  protected DistributedEngineBuilder(
      String name, DistributedEngineConfig config, PrimitiveManagementService managementService) {
    super(DistributedEngineType.instance(), name, config, managementService);
  }
}
