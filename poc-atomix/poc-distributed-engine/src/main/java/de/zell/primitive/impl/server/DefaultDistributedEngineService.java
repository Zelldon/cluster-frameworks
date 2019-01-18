package de.zell.primitive.impl.server;

import static de.zell.Broker.ROOT_DIR;

import de.zell.primitive.DistributedEngineType;
import de.zell.primitive.api.client.DistributedEngineClient;
import de.zell.primitive.api.server.DistributedEngineService;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedEngineService
    extends AbstractPrimitiveService<DistributedEngineClient> implements DistributedEngineService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedEngineService.class);
  private long position;
  //  private BufferedWriter bufferedWriter;
  private File EngineFile;
  private FileChannel fileChannel;

  public DefaultDistributedEngineService() {
    super(DistributedEngineType.instance(), DistributedEngineClient.class);

    position = 0L;
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);

    final String nodeId = this.getLocalMemberId().id();
    LOG.info("Current session member id {}", nodeId);
    final String serviceName = this.getServiceName();
    LOG.info("Service name: {}", serviceName);
    final Long serviceId = this.getServiceId().id();
    LOG.info("Service id: {}", serviceId);

    final File directory = new File(ROOT_DIR, nodeId);
    directory.mkdirs();

    final String fileName = new StringBuilder(serviceName).append("-").append(serviceId).toString();
    EngineFile = new File(directory, fileName);
    try {
      EngineFile.createNewFile();

      RandomAccessFile raf = new RandomAccessFile(EngineFile, "rw");
      fileChannel = raf.getChannel();

      //      bufferedWriter = Files.newWriter(EngineFile, Charset.defaultCharset());
    } catch (IOException e) {
      LOG.error("Error on creating new writer", e);
      e.printStackTrace();
    }
  }

  @Override
  public void newWorkflowInstance(String workflowId) {
    LOG.info("#newWorkflowInstance({}): current index {}.", workflowId, this.getCurrentIndex());

    final Session<DistributedEngineClient> currentSession = getCurrentSession();
    if (workflowId.contains("fail")) {
      currentSession.accept(
          distributedEngineClient -> {
            final String reason =
                String.format(
                    "Expected to find workflow with id %s, but does not exist.", workflowId);
            distributedEngineClient.rejectWorkflowInstanceCreation(reason);
          });
    } else {
      currentSession.accept(
          distributedEngineClient -> {
            distributedEngineClient.createdWorkflowInstance(getCurrentIndex());
          });
    }
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.info("#backup(BackupOutput): current index {}", this.getCurrentIndex());
    LOG.info("Do an backup of the current position {}.", position);
    backupOutput.writeLong(position);
    backupOutput.writeObject("FINDME");
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.info("#restore(BackupInput): current index {}", this.getCurrentIndex());
    LOG.info("Restore an backup of an previous state.");
    position = backupInput.readLong();
    backupInput.readObject();
    LOG.info("Restored position: {}", position);

    // position could consist of [file id (int32) | file offset (int32)]
    // if we have an offset larger then zero we need to copy the content and rewrite it so we
    // we can write on the end of the file.
    //  Files.write(Paths.get("myfile.txt"), "the text".getBytes(), StandardOpenOption.APPEND)

  }
}
