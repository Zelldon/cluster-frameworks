package de.zell.logstream;


import static de.zell.Primitive.ROOT_DIR;

import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
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
  private final File logstreamFile;
  private int position;
  private BufferedWriter bufferedWriter;

  public DefaultDistributedLogstreamService() {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);

    final File directory = new File(ROOT_DIR, this.getCurrentSession().memberId().id());
    directory.mkdirs();

    logstreamFile = new File(directory, "logstream");
    try {
      logstreamFile.createNewFile();
      bufferedWriter = Files.newWriter(logstreamFile, Charset.defaultCharset());
    } catch (IOException e) {
      e.printStackTrace();
    }
    position = 0;
  }

  @Override
  public void append(byte[] bytes) {
    LOG.info("Append given bytes {}", Arrays.toString(bytes));
    try {
      bufferedWriter.write(Arrays.toString(bytes), position, bytes.length);
      position += bytes.length;
    } catch (IOException e) {
      e.printStackTrace();
    }
    // to append in log stream impl
    Session<DistributedLogstreamClient> currentSession = getCurrentSession();
    final long position = 0xCAFE;
    currentSession.accept(
        distributedLogstreamClient -> distributedLogstreamClient.appended(position));
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.debug("Do an backup of the current position {}.", position);
    backupOutput.writeInt(position);
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.debug("Restore an backup of an previous state.");
    position = backupInput.readInt();
    LOG.debug("Restored position: {}", position);
  }
}
