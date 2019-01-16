package de.zell.logstream;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.proxy.ProxyClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.agrona.DirectBuffer;

public class DistributedLogstreamProxy
    extends AbstractAsyncPrimitive<AsyncDistributedLogstream, DistributedLogstreamService>
    implements AsyncDistributedLogstream, DistributedLogstreamClient {
  private volatile CompletableFuture<Long> appendFuture;

  public DistributedLogstreamProxy(
      ProxyClient<DistributedLogstreamService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public CompletableFuture<Long> append(DirectBuffer bytes) {
    appendFuture = new CompletableFuture<>();

    // TODO need to copy given bytes

    getProxyClient()
        .acceptBy(name(), service -> service.append(bytes))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
              }
            });
    return appendFuture.thenApply(result -> result).whenComplete((r, e) -> appendFuture = null);
  }

  @Override
  public DistributedLogstream sync() {
    return new BlockingLogstream(this, 1000);
  }

  @Override
  public SyncPrimitive sync(Duration duration) {
    return new BlockingLogstream(this, duration.toMillis());
  }

  @Override
  public void appended(long position) {
    CompletableFuture<Long> appendFuture = this.appendFuture;
    if (appendFuture != null) {
      appendFuture.complete(position);
    }
  }
  //
  //    @Override
  //    public void failed() {
  //      CompletableFuture<Optional<Long>> appendFuture = this.appendFuture;
  //      if (appendFuture != null) {
  //        appendFuture.complete(Optional.empty());
  //      }
  //    }

}
