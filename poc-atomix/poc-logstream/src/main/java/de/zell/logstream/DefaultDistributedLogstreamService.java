package de.zell.logstream;

import static de.zell.Primitive.ROOT_DIR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import com.google.common.io.Files;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);
  private int position;
  private BufferedWriter bufferedWriter;
  private File logstreamFile;

  public DefaultDistributedLogstreamService() {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);

    position = 0;
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);

    String id = this.getLocalMemberId().id();
    LOG.info("Current session member id {}", id);
    final File directory = new File(ROOT_DIR, id);
    directory.mkdirs();

    logstreamFile = new File(directory, "logstream");
    try {
      logstreamFile.createNewFile();
      bufferedWriter = Files.newWriter(logstreamFile, Charset.defaultCharset());
    } catch (IOException e) {
      LOG.error("Error on creating new writer", e);
      e.printStackTrace();
    }
  }

  @Override
  public void append(byte[] bytes) {
    LOG.info("#append(byte[]): current index {}", this.getCurrentIndex());
    LOG.info("Append given bytes {}", Arrays.toString(bytes));
    try {
      bufferedWriter.write(Arrays.toString(bytes), 0, bytes.length);
      position += bytes.length;
      bufferedWriter.flush();
    } catch (IOException e) {
      LOG.error("Error on write", e);
      e.printStackTrace();
    }
    // to append in log stream impl
    Session<DistributedLogstreamClient> currentSession = getCurrentSession();
    currentSession.accept(
        distributedLogstreamClient -> distributedLogstreamClient.appended(position));
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.info("#backup(BackupOutput): current index {}", this.getCurrentIndex());
    LOG.debug("Do an backup of the current position {}.", position);
    backupOutput.writeInt(position);
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.info("#restore(BackupInput): current index {}", this.getCurrentIndex());
    LOG.debug("Restore an backup of an previous state.");
    position = backupInput.readInt();
    LOG.debug("Restored position: {}", position);

    // position could consist of [file id (int32) | file offset (int32)]
    // if we have an offset larger then zero we need to copy the content and rewrite it so we
    // we can write on the end of the file.
    //  Files.write(Paths.get("myfile.txt"), "the text".getBytes(), StandardOpenOption.APPEND)

  }
}
