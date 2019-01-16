package de.zell.logstream;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogstreamProxy
    extends AbstractAsyncPrimitive<AsyncDistributedLogstream, DistributedLogstreamService>
    implements AsyncDistributedLogstream, DistributedLogstreamClient {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogstreamProxy.class);

  public static final long DEFAULT_TIMEOUT = 15_000L;

  private volatile CompletableFuture<Long> appendFuture;

  public DistributedLogstreamProxy(
      ProxyClient<DistributedLogstreamService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public CompletableFuture<Long> append(DirectBuffer bytes) {
    appendFuture = new CompletableFuture<>();

    LOG.debug("Get proxy and try to append bytes: {}.", Arrays.toString(bytes.byteArray()));
    // TODO need to copy given bytes

    getProxyClient()
        .acceptBy(name(), service -> service.append(bytes))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
                LOG.debug("Append completed with an error.", error);
              }
              else
              {
                LOG.debug("Append was successful.");
              }

            });
    return appendFuture.thenApply(result -> result)
                       .whenComplete((r, e) -> appendFuture = null);
  }

  @Override
  public DistributedLogstream sync() {
    return sync(Duration.ofMillis(DEFAULT_TIMEOUT));
  }

  @Override
  public DistributedLogstream sync(Duration duration) {
    return new BlockingLogstream(this, duration.toMillis());
  }

  @Override
  public void appended(long position) {
    CompletableFuture<Long> appendFuture = this.appendFuture;
    if (appendFuture != null) {
      LOG.debug("Bytes were appended at position {}.", position);
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
