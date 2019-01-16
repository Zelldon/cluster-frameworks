package de.zell.logstream;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;

public abstract class DistributedLogstreamBuilder
    extends PrimitiveBuilder<DistributedLogstreamBuilder, DistributedLogstreamConfig, DistributedLogstream>
    implements ProxyCompatibleBuilder<DistributedLogstreamBuilder> {

  protected DistributedLogstreamBuilder(String name, DistributedLogstreamConfig config, PrimitiveManagementService managementService) {
      super(DistributedLogstreamType.instance(), name, config, managementService);
    }
}
