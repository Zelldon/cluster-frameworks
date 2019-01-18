package de.zell.primitive.impl.client;

import de.zell.primitive.api.client.AsyncDistributedEngine;
import de.zell.primitive.api.client.DistributedEngine;
import de.zell.primitive.api.client.DistributedEngineClient;
import de.zell.primitive.api.server.DistributedEngineService;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedEngineProxy
    extends AbstractAsyncPrimitive<AsyncDistributedEngine, DistributedEngineService>
    implements AsyncDistributedEngine, DistributedEngineClient {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedEngineProxy.class);

  public static final long DEFAULT_TIMEOUT = 15_000L;

  private volatile CompletableFuture<Long> appendFuture;

  public DistributedEngineProxy(
      ProxyClient<DistributedEngineService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public CompletableFuture<Long> newWorkflowInstance(String workflowId) {

    PrimitiveState state = getProxyClient().getPartition(name()).getState();
    if (state != PrimitiveState.CONNECTED) {
      LOG.error("Proxy client is currently not connected.");
      return CompletableFuture.completedFuture(-1L);
    }

    appendFuture = new CompletableFuture<>();
    LOG.debug("Get proxy and try to append bytes.");
    // TODO need to copy given bytes

    getProxyClient()
        .acceptBy(name(), service -> service.newWorkflowInstance(workflowId))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
                LOG.error("Append completed with an error.", error);
              } else {
                LOG.debug("Append was successful.");
              }
            });
    return appendFuture.thenApply(result -> result).whenComplete((r, e) -> appendFuture = null);
  }

  @Override
  public DistributedEngine sync() {
    return sync(Duration.ofMillis(DEFAULT_TIMEOUT));
  }

  @Override
  public DistributedEngine sync(Duration duration) {
    return new BlockingEngine(this, duration.toMillis());
  }

  @Override
  public void createdWorkflowInstance(long position) {
    CompletableFuture<Long> appendFuture = this.appendFuture;
    if (appendFuture != null) {
      LOG.info("Bytes were appended at position {}.", position);
      appendFuture.complete(position);
    }
  }

  @Override
  public void rejectWorkflowInstanceCreation(String reason) {
    LOG.error("Workflow instance creation was rejection, reason {}.", reason);
  }

  @Override
  public CompletableFuture<AsyncDistributedEngine> connect() {
    return super.connect()
        .thenCompose(
            v ->
                Futures.allOf(
                    this.getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenApply(v -> this);
  }
}
