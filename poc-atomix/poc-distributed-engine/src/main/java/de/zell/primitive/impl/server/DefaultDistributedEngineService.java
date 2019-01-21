package de.zell.primitive.impl.server;

import java.util.HashSet;
import java.util.Set;

import de.zell.primitive.DistributedEngineType;
import de.zell.primitive.api.client.DistributedEngineClient;
import de.zell.primitive.api.server.DistributedEngineService;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedEngineService
    extends AbstractPrimitiveService<DistributedEngineClient> implements DistributedEngineService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedEngineService.class);
  private Set<Long> workflowInstances = new HashSet<>();

  public DefaultDistributedEngineService() {
    super(DistributedEngineType.instance(), DistributedEngineClient.class);
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
  }

  @Override
  public void newWorkflowInstance(String workflowId) {
    LOG.info("#newWorkflowInstance({}): current index {}.", workflowId, this.getCurrentIndex());

    final Session<DistributedEngineClient> currentSession = getCurrentSession();

    currentSession.accept(
        distributedEngineClient -> {
          if (workflowId.contains("fail")) {
            final String reason =
                String.format(
                    "Expected to find workflow with id %s, but does not exist.", workflowId);

            LOG.error(reason);
            distributedEngineClient.rejectWorkflowInstanceCreation(reason);
          } else {
            final long workflowInstanceId = getCurrentIndex();
            LOG.info("Workflow instance {} created.", workflowInstanceId);
            workflowInstances.add(workflowInstanceId);
            distributedEngineClient.createdWorkflowInstance(workflowInstanceId);
          }
        });
  }

  @Override
  public void executeStartEvent(long workflowInstanceId) {
    LOG.info(
        "#executeStartEvent({}): current index {}.", workflowInstanceId, this.getCurrentIndex());

    final Session<DistributedEngineClient> currentSession = getCurrentSession();

    currentSession.accept(
        distributedEngineClient -> {
          if (workflowInstances.contains(workflowInstanceId)) {
            LOG.info("Start event for workflow instance {} executed.", workflowInstanceId);
            distributedEngineClient.startEventExecuted(workflowInstanceId);
          } else {
            rejectActivityExecution(workflowInstanceId, distributedEngineClient);
          }
        });
  }

  @Override
  public void executeEndEvent(long workflowInstanceId) {
    LOG.info("#executeEndEvent({}): current index {}.", workflowInstanceId, this.getCurrentIndex());

    final Session<DistributedEngineClient> currentSession = getCurrentSession();

    currentSession.accept(
        distributedEngineClient -> {
          if (workflowInstances.contains(workflowInstanceId)) {
            LOG.info("End event for workflow instance {} executed.", workflowInstanceId);
            distributedEngineClient.endEventExecuted(workflowInstanceId);
          } else {
            rejectActivityExecution(workflowInstanceId, distributedEngineClient);
          }
        });
  }

  private void rejectActivityExecution(
      long workflowInstanceId, DistributedEngineClient distributedEngineClient) {
    final String reason =
        String.format(
            "Expected to find workflow instance with id %d, but does not exist.",
            workflowInstanceId);
    LOG.error(reason);
    distributedEngineClient.rejectActivityExecution(reason);
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    LOG.info("#backup(BackupOutput): current index {}", this.getCurrentIndex());
    backupOutput.writeObject("FINDME");
  }

  @Override
  public void restore(BackupInput backupInput) {
    LOG.info("#restore(BackupInput): current index {}", this.getCurrentIndex());
    LOG.info("Restore an backup of an previous state.");
    backupInput.readObject();
  }
}
