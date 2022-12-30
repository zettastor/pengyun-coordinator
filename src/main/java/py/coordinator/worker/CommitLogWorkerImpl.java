/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.coordinator.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.BogusSegId;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.RequestIdBuilder;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.response.CommitLogCallback;
import py.coordinator.response.CommitLogResponseCollector;
import py.coordinator.task.CommitLogContext;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.SegmentMembership;
import py.netty.datanode.AsyncDataNode;
import py.netty.exception.DisconnectionException;
import py.proto.Broadcastlog;
import py.proto.Commitlog;
import py.proto.Commitlog.RequestType;
import py.volume.VolumeType;


public class CommitLogWorkerImpl implements CommitLogWorker {

  private static final Logger logger = LoggerFactory.getLogger(CommitLogWorkerImpl.class);
  private static final long DEFAULT_COMMIT_LOG_DELAY_TIME_MS = 1000;
  private static final AtomicLong commitLogWorkRequestIdBuilder = new AtomicLong(Long.MIN_VALUE);
 
  private static final int MAX_IO_MANAGER_CAN_COMMIT_AT_ONE_TIME = 10;
  private final Thread commitLogThread;
  private final DelayQueue<CommitLogContext> taskQueue;
  private final Map<SegId, AtomicReference<CommitWorkProgress>> taskRecorder;
  private final Coordinator coordinator;
  private final String className;


  
  public CommitLogWorkerImpl(Coordinator coordinator) {
    this.coordinator = coordinator;
    this.taskQueue = new DelayQueue<>();
    this.taskRecorder = new ConcurrentHashMap<>();
    this.className = getClass().getSimpleName();
    this.commitLogThread = new Thread("commit-log-thread") {
      @Override
      public void run() {
        try {
          commitLogs();
        } catch (Throwable t) {
          logger.error("commit log thread can not deal with io context manager", t);
        }
      }
    };
  }

  private void commitLogs() throws Exception {
    List<CommitLogContext> tasks = new ArrayList<>();
    List<SegId> processedSegId = new ArrayList<>();
    while (true) {
      tasks.clear();
      processedSegId.clear();

     
      if (taskQueue.drainTo(tasks) == 0) {
       
        tasks.add(taskQueue.take());
      }

     
      for (CommitLogContext commitLogContext : tasks) {
        Long volumeId = commitLogContext.getVolumeId();
        SegId segId = commitLogContext.getSegId();
        if (segId instanceof BogusSegId) {
          logger.warn("exit commit log worker");
          return;
        }
        if (processedSegId.contains(segId)) {
          Validate.isTrue(false, "can not have repeat segId:{} task", segId);
        }
        processedSegId.add(segId);

        try {
          AtomicReference<CommitWorkProgress> commitWorkProgressAtomicReference = taskRecorder
              .get(segId);
          Validate
              .isTrue(commitWorkProgressAtomicReference.get() == CommitWorkProgress.CommitWaiting);

          List<WriteIoContextManager> allManagersInOneSegment = coordinator
              .getUncommittedLogManager()
              .pollLogManagerToCommit(volumeId, segId);
          if (allManagersInOneSegment.isEmpty()) {
            commitWorkProgressAtomicReference.getAndSet(CommitWorkProgress.Commitable);
            continue;
          }

          commitWorkProgressAtomicReference.getAndSet(CommitWorkProgress.Committing);

          int commitCount = 0;
          int beginPosition = 0;
          int leftManagerCount = allManagersInOneSegment.size();
          boolean lastRound = false;
          while (leftManagerCount > 0) {
            int incNumber = MAX_IO_MANAGER_CAN_COMMIT_AT_ONE_TIME > leftManagerCount
                ? leftManagerCount
                : MAX_IO_MANAGER_CAN_COMMIT_AT_ONE_TIME;
            final List<WriteIoContextManager> subCommitManagers = allManagersInOneSegment
                .subList(beginPosition, beginPosition + incNumber);
            beginPosition += incNumber;
            leftManagerCount -= incNumber;
            if (leftManagerCount == 0) {
              lastRound = true;
            }
            if (processCommitManagers(subCommitManagers, volumeId, segId, lastRound)) {
              commitCount++;
            }
            Validate.isTrue(subCommitManagers.size() == incNumber);
            Validate.isTrue(incNumber <= MAX_IO_MANAGER_CAN_COMMIT_AT_ONE_TIME);
          }
          Validate.isTrue(leftManagerCount == 0);

          if (commitCount == 0) {
            commitWorkProgressAtomicReference.getAndSet(CommitWorkProgress.Commitable);
           
          }
        } catch (Exception e) {
          logger.warn("caught an exception when committing:{} logs", segId, e);
        }
      }
    }
  }


  
  public void stop() {
    try {
      logger.warn("try to stop commit log worker");
      taskQueue.offer(new CommitLogContext(0L, new BogusSegId(), 0));
      commitLogThread.join(1000);
    } catch (InterruptedException e) {
      logger.error("failed to exit commit log thread", e);
    }
  }

  public void start() {
    commitLogThread.start();
  }


  
  public boolean put(Long volumeId, SegId segId, boolean needDelay) {
    if (!taskRecorder.containsKey(segId)) {
      AtomicReference<CommitWorkProgress> atomicReference = new AtomicReference<>(
          CommitWorkProgress.Commitable);
      taskRecorder.putIfAbsent(segId, atomicReference);
    }

    if (taskRecorder.get(segId)
        .compareAndSet(CommitWorkProgress.Commitable, CommitWorkProgress.CommitWaiting)) {
      long delay = 0L;
      if (needDelay) {
        delay = DEFAULT_COMMIT_LOG_DELAY_TIME_MS;
      }
      taskQueue.offer(new CommitLogContext(volumeId, segId, delay));
      return true;
    }
    return false;
  }

  private boolean processCommitManagers(List<WriteIoContextManager> subCommitManagers,
      Long volumeId, SegId segId,
      boolean lastRound) {
    if (subCommitManagers == null || subCommitManagers.isEmpty()) {
      logger.info("do not need to process commit managers at :{}", segId);
      return false;
    }

    SegmentMembership membership = coordinator.getSegmentMembership(volumeId, segId);
    final VolumeType volumeType = coordinator.getVolumeType(volumeId);

    Commitlog.PbCommitlogRequest.Builder requestBuilder = Commitlog.PbCommitlogRequest.newBuilder();
    requestBuilder.setType(RequestType.COMMIT);
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setVolumeId(segId.getVolumeId().getId());
    requestBuilder.setSegIndex(segId.getIndex());
    requestBuilder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));

    List<Broadcastlog.PbBroadcastLogManager> broadcastManagers = new ArrayList<>();
    List<Broadcastlog.PbBroadcastLog> broadcastLogs = new ArrayList<>();

    for (WriteIoContextManager manager : subCommitManagers) {
      if (manager.isAllFinalStatus()) {
        Validate.isTrue(false, "sub commit manager: " + manager);
      }
      Broadcastlog.PbBroadcastLogManager.Builder managerBuilder = Broadcastlog.PbBroadcastLogManager
          .newBuilder();
      managerBuilder.setRequestId(manager.getRequestId());
      for (BroadcastLog broadcastLog : manager.getLogsToCommit()) {
       
        if (broadcastLog.getLogId() <= 0) {
          Validate.isTrue(false,
              "ori:" + manager.getOriRequestId() + " got log:" + broadcastLog.getLogUuid()
                  + "has logId:"
                  + broadcastLog.getLogId());
        }
        broadcastLogs.add(PbRequestResponsePbHelper.buildPbBroadcastLogFrom(broadcastLog));
      }

      managerBuilder.addAllBroadcastLogs(broadcastLogs);
      broadcastManagers.add(managerBuilder.build());
      broadcastLogs.clear();
    }

    requestBuilder.addAllBroadcastManagers(broadcastManagers);

    Long requestId = commitLogWorkRequestIdBuilder.incrementAndGet();
    IoActionContext ioActionContext = coordinator.getWriteFactory()
        .generateIoMembers(membership, volumeType, segId, requestId);

    if (ioActionContext.isResendDirectly()) {
      logger.info("try to commit log at:{}, but should resend directly", segId);
      coordinator.getUncommittedLogManager().addLogManagerToCommit(subCommitManagers);
      return false;
    }

   
    AtomicInteger commitCount = new AtomicInteger(ioActionContext.getIoMembers().size());
    CommitLogCallback[] callbacks = new CommitLogCallback[commitCount.get()];
    CommitLogResponseCollector responseCollector = new CommitLogResponseCollector(requestId,
        membership, volumeId,
        segId, coordinator, subCommitManagers, callbacks, commitCount, ioActionContext,
        CoordinatorConfigSingleton.getInstance().isStreamIo(), lastRound);
    LoggerTracer.getInstance()
        .mark(responseCollector.getRequestId(), className, "ori:{} begin to commit log",
            responseCollector.getRequestId());

   
    int index = 0;
    Commitlog.PbCommitlogRequest request = requestBuilder.build();
    for (IoMember ioMember : ioActionContext.getIoMembers()) {
      CommitLogCallback commitLogCallback = new CommitLogCallback(coordinator, volumeId, segId,
          responseCollector,
          ioMember, taskRecorder);
      try {
        callbacks[index++] = commitLogCallback;
        AsyncDataNode.AsyncIface client = coordinator
            .getDatanodeNettyClient(ioMember.getInstanceId());
        
        if (client == null) {
          logger.warn(
              "can not get:{} @:{} @:{} for commit logs, target will be treated as disconnected.",
              ioMember.getInstanceId(), ioMember.getEndPoint().getHostName(), segId);
          throw new DisconnectionException();
        }
       
        logger.info("ori:{} going to commit log to:{}", responseCollector.getOriRequestId(),
            ioMember.getEndPoint());
        client.addOrCommitLogs(request, commitLogCallback);
      } catch (Exception e) {
        logger.warn("try commit logs to datanode:{} unsuccessfully, segId: {}",
            ioMember.getEndPoint(), segId,
            e);
        commitLogCallback.fail(e);
      }
    }
    return true;
  }

}
