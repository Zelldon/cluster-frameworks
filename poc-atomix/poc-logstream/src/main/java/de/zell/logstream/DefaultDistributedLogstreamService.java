package de.zell.logstream;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import sun.nio.ch.DirectBuffer;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {
  private Queue<SessionId> queue = new ArrayDeque<>();
  private SessionId lock;

  public DefaultDistributedLogstreamService() {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
  }

  @Override
  public void append(DirectBuffer bytes) {

    // to append in log stream impl
    Session<DistributedLogstreamClient> currentSession = getCurrentSession();
    final long position = 0xCAFE;
    currentSession.accept(
        distributedLogstreamClient -> distributedLogstreamClient.appended(position));
  }

  @Override
  public void backup(BackupOutput backupOutput) {}

  @Override
  public void restore(BackupInput backupInput) {}
}
