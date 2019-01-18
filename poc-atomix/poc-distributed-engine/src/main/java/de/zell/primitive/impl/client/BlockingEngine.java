package de.zell.primitive.impl.client;

import de.zell.primitive.api.client.AsyncDistributedEngine;
import de.zell.primitive.api.client.DistributedEngine;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.atomix.primitive.Synchronous;

public class BlockingEngine extends Synchronous<AsyncDistributedEngine>
    implements DistributedEngine {

  private final DistributedEngineProxy distributedEngineProxy;
  private final long timeout;

  public BlockingEngine(DistributedEngineProxy distributedEngineProxy, long timeout) {
    super(distributedEngineProxy);
    this.distributedEngineProxy = distributedEngineProxy;
    this.timeout = timeout;
  }

  @Override
  public long newWorkflowInstance(String workflowId) {

    // blocking
    long position = -1;
    try {
      position = distributedEngineProxy.newWorkflowInstance(workflowId).get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
    return position;
  }

  @Override
  public AsyncDistributedEngine async() {
    return distributedEngineProxy;
  }
}
