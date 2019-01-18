package de.zell.primitive.impl.client;

import de.zell.primitive.DistributedEngineBuilder;
import de.zell.primitive.DistributedEngineConfig;
import de.zell.primitive.api.client.AsyncDistributedEngine;
import de.zell.primitive.api.client.DistributedEngine;
import de.zell.primitive.api.server.DistributedEngineService;
import de.zell.primitive.impl.client.DistributedEngineProxy;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.service.ServiceConfig;

public class DistributedEngineProxyBuilder extends DistributedEngineBuilder {

  public DistributedEngineProxyBuilder(
      String name, DistributedEngineConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedEngine> buildAsync() {
    return newProxy(DistributedEngineService.class, new ServiceConfig())
        .thenCompose(
            proxyClient ->
                new DistributedEngineProxy(proxyClient, managementService.getPrimitiveRegistry())
                    .connect())
        .thenApply(AsyncDistributedEngine::sync);
  }

  @Override
  public DistributedEngineBuilder withProtocol(ProxyProtocol proxyProtocol) {
    return this.withProtocol((PrimitiveProtocol) proxyProtocol);
  }
}
