package de.zell.logstream;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;

public class DistributedLogstreamType
    implements PrimitiveType<
        DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream> {

  public static PrimitiveType instance() {
    return new DistributedLogstreamType();
  }

  @Override
  public DistributedLogstreamConfig newConfig() {
    return new DistributedLogstreamConfig();
  }

  @Override
  public DistributedLogstreamBuilder newBuilder(String name,
      DistributedLogstreamConfig distributedLogStreamConfig,
      PrimitiveManagementService primitiveManagementService) {
    return new DistributedLogstreamProxyBuilder(name, distributedLogStreamConfig, primitiveManagementService);
  }

  @Override
  public PrimitiveService newService(ServiceConfig serviceConfig) {
    return new DefaultDistributedLogstreamService();
  }

  @Override
  public String name() {
    return "logstream";
  }
}
