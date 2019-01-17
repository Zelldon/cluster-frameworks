package de.zell.engine;

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
  public long append(byte[] bytes) {

    // blocking
    long position = -1;
    try {
      position = distributedEngineProxy.append(bytes).get(timeout, TimeUnit.MILLISECONDS);
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
