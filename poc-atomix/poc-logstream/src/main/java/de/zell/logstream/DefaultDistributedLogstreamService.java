package de.zell.logstream;

import static de.zell.Primitive.ROOT_DIR;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbString;
import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  private File logstreamFile;
  private FileChannel fileChannel;
  private ZeebeDb<DefaultColumnFamily> db;
  private File rocksDbDir;
  private DbString key;
  private ColumnFamily<DbString, DbLong> positionColumnFamily;
  private DbLong position;

  public DefaultDistributedLogstreamService() {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);

    String name = "logstream";
    try {
      final Field context = DefaultServiceExecutor.class.getDeclaredField("context");
      context.setAccessible(true);
      final RaftServiceContext raftServiceContext = (RaftServiceContext) context.get(executor);
      final Field raft = RaftServiceContext.class.getDeclaredField("raft");
      raft.setAccessible(true);
      RaftContext raftContext = (RaftContext) raft.get(raftServiceContext);
      name = raftContext.getName();
      LOG.error("configure {}", name);
      raft.setAccessible(false);
      context.setAccessible(false);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    final String nodeId = this.getLocalMemberId().id();
    LOG.info("Current session member id {}", nodeId);
    final String serviceName = this.getServiceName();
    LOG.info("Service name: {}", serviceName);
    final Long serviceId = this.getServiceId().id();
    LOG.info("Service id: {}", serviceId);

    final File directory = new File(ROOT_DIR, nodeId);
    directory.mkdirs();

    final File partitionDir = new File(directory, name);
    partitionDir.mkdir();
    logstreamFile = new File(partitionDir, "logstream");
    try {
      logstreamFile.createNewFile();

      RandomAccessFile raf = new RandomAccessFile(logstreamFile, "rw");
      fileChannel = raf.getChannel();

      //      bufferedWriter = Files.newWriter(logstreamFile, Charset.defaultCharset());
    } catch (IOException e) {
      LOG.error("Error on creating new writer", e);
      e.printStackTrace();
    }

    if (db == null) {
      try {
        rocksDbDir = new File(partitionDir, "rocksDb");
        initDatabase();
      } catch (Exception e) {
        LOG.error("Error on opening rocks db", e);
        throw e;
      }
    }
  }

  private void initDatabase() {
    db = ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class).createDb(rocksDbDir);
    key = new DbString();
    key.wrapString("position");

    position = new DbLong();
    positionColumnFamily = db.createColumnFamily(DefaultColumnFamily.DEFAULT, key, position);
  }

  @Override
  public void append(byte[] bytes) {
    LOG.info(
        "#append(byte[]): current index {} position {}",
        this.getCurrentIndex(),
        position.getValue());
    LOG.debug("Append given bytes.");

    try {
      final long newPosition = appendToLogstream(bytes);

      // to append in log stream impl
      Session<DistributedLogstreamClient> currentSession = getCurrentSession();
      //      currentSession.publish(PrimitiveEvent.event(EventType.from(""), bytes));
      currentSession.accept(
          distributedLogstreamClient -> distributedLogstreamClient.appended(newPosition));
    } catch (IOException e) {
      LOG.error("Error on append", e);
      e.printStackTrace();
    }
  }

  private long appendToLogstream(byte[] bytes) throws IOException {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    fileChannel.write(byteBuffer, position.getValue());
    //    bufferedWriter.write(Arrays.toString(bytes), 0, bytes.length);
    position.wrapLong(position.getValue() + bytes.length);
    positionColumnFamily.put(key, position);
    //    bufferedWriter.flush();
    return position.getValue();
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.info("#backup(BackupOutput): current index {}", this.getCurrentIndex());
    LOG.info("Do an backup of the current position {}.", position.getValue());

    final File snapshotDir = new File(rocksDbDir, "snapshot");
    db.createSnapshot(snapshotDir);

    try {
      Files.list(snapshotDir.toPath())
          .forEach(
              (file) -> {
                backupOutput.writeString(file.getFileName().toString());
                try {
                  final byte[] bytes = Files.readAllBytes(file);
                  backupOutput.writeInt(bytes.length);
                  backupOutput.writeBytes(bytes);
                } catch (IOException e) {
                  LOG.error("Error on reading all bytes.", e);
                  e.printStackTrace();
                }

                try {
                  Files.delete(file);
                } catch (IOException e) {
                  LOG.error("Error on deleting file.", e);
                  e.printStackTrace();
                }
              });
      Files.delete(snapshotDir.toPath());
    } catch (IOException e) {
      LOG.error("Error on listing all files.", e);
      e.printStackTrace();
    }
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.info("#restore(BackupInput): current index {}", this.getCurrentIndex());
    LOG.info("Restore an backup of an previous state.");

    // position could consist of [file id (int32) | file offset (int32)]
    // if we have an offset larger then zero we need to copy the content and rewrite it so we
    // we can write on the end of the file.
    //  Files.write(Paths.get("myfile.txt"), "the text".getBytes(), StandardOpenOption.APPEND)

    try {
      db.close();
    } catch (Exception e) {
      LOG.error("Error on closing database.", e);
      e.printStackTrace();
    }
    while (backupInput.hasRemaining()) {
      final String fileName = backupInput.readString();
      final File snapshottedFile = new File(rocksDbDir, fileName);
      int length = backupInput.readInt();
      try {
        Files.write(
            snapshottedFile.toPath(),
            backupInput.readBytes(length),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
      } catch (IOException e) {
        LOG.error("Error on writing to file.", e);
        e.printStackTrace();
      }
    }

    initDatabase();

    final DbLong dbLong = positionColumnFamily.get(key);
    LOG.info("Restored position: {}", dbLong.getValue());
  }
}
