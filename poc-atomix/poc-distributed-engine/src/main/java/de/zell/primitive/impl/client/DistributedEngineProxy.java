package de.zell.primitive.impl.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import de.zell.primitive.api.client.AsyncDistributedEngine;
import de.zell.primitive.api.client.DistributedEngine;
import de.zell.primitive.api.client.DistributedEngineClient;
import de.zell.primitive.api.server.DistributedEngineService;
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

  private volatile CompletableFuture<Long> newWfInstanceFuture;
  private CompletableFuture<Long> activityExecutedFuture;

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

    getProxyClient()
        .addStateChangeListener(
            (s) -> {
              if (PrimitiveState.EXPIRED == s
                  || PrimitiveState.CLOSED == s
                  || PrimitiveState.SUSPENDED == s) {
                LOG.error("try Reconnect");
                connect();
              }
            });

    newWfInstanceFuture = new CompletableFuture<>();

    getProxyClient()
        .acceptBy(name(), service -> service.newWorkflowInstance(workflowId))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                newWfInstanceFuture.completeExceptionally(error);
              }
            });

    return newWfInstanceFuture
        .thenApply(result -> result)
        .whenComplete((r, e) -> newWfInstanceFuture = null);
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

    // workflow instance created
    completeFuture(newWfInstanceFuture, position, "Workflow instance was created at {}.");

    final long workflowId = position;

    // next step -> start event
    activityExecutedFuture = new CompletableFuture<>();
    getProxyClient()
        .acceptBy(name(), service -> service.executeStartEvent(workflowId))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                activityExecutedFuture.completeExceptionally(error);
              }
            });
  }

  @Override
  public void rejectWorkflowInstanceCreation(String reason) {
    newWfInstanceFuture.completeExceptionally(new RuntimeException(reason));
    LOG.error("Workflow instance creation was rejection, reason {}.", reason);
  }

  @Override
  public void startEventExecuted(long workflowId) {
    completeFuture(
        activityExecutedFuture, workflowId, "Start event of workflow instance ({}) was executed.");

    // find next steps/activities
    // -> end event
    activityExecutedFuture = new CompletableFuture<>();
    getProxyClient()
        .acceptBy(name(), service -> service.executeEndEvent(workflowId))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                activityExecutedFuture.completeExceptionally(error);
              }
            });
  }

  @Override
  public void endEventExecuted(long workflowId) {
    completeFuture(
        activityExecutedFuture, workflowId, "End event of workflow instance ({}) was executed.");
  }

  @Override
  public void rejectActivityExecution(String reason) {
    LOG.error("Execution of activity was rejected, because {}", reason);
    activityExecutedFuture.completeExceptionally(new RuntimeException(reason));
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

  private void completeFuture(CompletableFuture<Long> future, long position, String logMsg) {
    if (future != null) {
      LOG.info(logMsg, position);
      future.complete(position);
    }
  }
}
