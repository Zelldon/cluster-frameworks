package de.zell.logstream;

import java.util.Arrays;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  public DefaultDistributedLogstreamService() {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
  }

  @Override
  public void append(byte[] bytes) {
    LOG.info("Append given bytes {}", Arrays.toString(bytes));

    // to append in log stream impl
    Session<DistributedLogstreamClient> currentSession = getCurrentSession();
    final long position = 0xCAFE;
    currentSession.accept(
        distributedLogstreamClient -> distributedLogstreamClient.appended(position));
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.debug("Do an backup of the current state.");
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.debug("Restore an backup of an previous state.");
  }
}
