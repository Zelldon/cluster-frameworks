package de.zell.primitive;

import de.zell.primitive.api.client.DistributedEngine;
import de.zell.primitive.impl.client.DistributedEngineProxyBuilder;
import de.zell.primitive.impl.server.DefaultDistributedEngineService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

public class DistributedEngineType
    implements PrimitiveType<DistributedEngineBuilder, DistributedEngineConfig, DistributedEngine> {

  public static PrimitiveType instance() {
    return new DistributedEngineType();
  }

  @Override
  public DistributedEngineConfig newConfig() {
    return new DistributedEngineConfig();
  }

  @Override
  public DistributedEngineBuilder newBuilder(
      String name,
      DistributedEngineConfig distributedEngineConfig,
      PrimitiveManagementService primitiveManagementService) {
    return new DistributedEngineProxyBuilder(
        name, distributedEngineConfig, primitiveManagementService);
  }

  @Override
  public PrimitiveService newService(ServiceConfig serviceConfig) {

    return new DefaultDistributedEngineService();
  }

  @Override
  public String name() {
    return "Engine";
  }
}
