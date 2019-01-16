package de.zell.logstream;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;
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
  public CompletableFuture<Long> append(byte[] bytes) {

    PrimitiveState state = getProxyClient().getPartition(name()).getState();
    if (state != PrimitiveState.CONNECTED) {
      LOG.error("Proxy client is currently not connected.");
      return CompletableFuture.completedFuture(-1L);
    }

    appendFuture = new CompletableFuture<>();
    LOG.info("Get proxy and try to append bytes: {}.", Arrays.toString(bytes));
    // TODO need to copy given bytes

    getProxyClient()
        .acceptBy(name(), service -> service.append(bytes))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                appendFuture.completeExceptionally(error);
                LOG.info("Append completed with an error.", error);
              } else {
                LOG.info("Append was successful.");
              }
            });
    return appendFuture.thenApply(result -> result).whenComplete((r, e) -> appendFuture = null);
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

  @Override
  public CompletableFuture<AsyncDistributedLogstream> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenApply(v -> (AsyncDistributedLogstream) this);
  }
}
