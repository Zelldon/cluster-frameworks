package de.zell.logstream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.atomix.primitive.Synchronous;

public class BlockingLogstream extends Synchronous<AsyncDistributedLogstream>
    implements DistributedLogstream {

  private final DistributedLogstreamProxy distributedLogstreamProxy;
  private final long timeout;

  public BlockingLogstream(DistributedLogstreamProxy distributedLogstreamProxy, long timeout) {
    super(distributedLogstreamProxy);
    this.distributedLogstreamProxy = distributedLogstreamProxy;
    this.timeout = timeout;
  }

  @Override
  public long append(byte[] bytes) {

    // blocking
    long position = -1;
    try {
      position = distributedLogstreamProxy.append(bytes).get(timeout, TimeUnit.MILLISECONDS);
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
  public AsyncDistributedLogstream async() {
    return distributedLogstreamProxy;
  }
}
