package de.zell.logstream;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.service.ServiceConfig;

public class DistributedLogstreamProxyBuilder extends DistributedLogstreamBuilder {

  public DistributedLogstreamProxyBuilder(
      String name,
      DistributedLogstreamConfig config,
      PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedLogstream> buildAsync() {
    return newProxy(DistributedLogstreamService.class, new ServiceConfig())
        .thenCompose(
            proxyClient ->
                new DistributedLogstreamProxy(proxyClient, managementService.getPrimitiveRegistry())
                    .connect())
        .thenApply(AsyncDistributedLogstream::sync);
  }

  @Override
  public DistributedLogstreamBuilder withProtocol(ProxyProtocol proxyProtocol) {
    return this.withProtocol((PrimitiveProtocol) proxyProtocol);
  }
}
