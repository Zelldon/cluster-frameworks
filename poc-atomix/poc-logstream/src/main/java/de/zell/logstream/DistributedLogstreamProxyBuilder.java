package de.zell.logstream;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.service.ServiceConfig;
import java.util.concurrent.CompletableFuture;

public class DistributedLogstreamProxyBuilder extends DistributedLogstreamBuilder {

  public DistributedLogstreamProxyBuilder(String name, DistributedLogstreamConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedLogstream> buildAsync() {
    return newProxy(DistributedLogstreamService.class, new ServiceConfig())
        .thenCompose(proxy -> new DistributedLogstreamProxy(proxy, managementService.getPrimitiveRegistry()).connect())
        .thenApply(logstream -> logstream.sync());
  }

  @Override
  public DistributedLogstreamBuilder withProtocol(ProxyProtocol proxyProtocol) {
    return this.withProtocol((PrimitiveProtocol) proxyProtocol);
  }
}
