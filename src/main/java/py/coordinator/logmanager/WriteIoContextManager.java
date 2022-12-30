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

package py.coordinator.logmanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.TraceAction;
import py.common.struct.Pair;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounit.ReadIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.SkipForSourceVolumeIoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.SkipForSourceVolumeIoUnitContextPacket;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.log.LogResult;
import py.coordinator.task.ResendRequest;
import py.icshare.BroadcastLogStatus;
import py.membership.IoMember;
import py.membership.SegmentForm;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.thrift.share.BroadcastLogThrift;


public class WriteIoContextManager extends AbstractIoContextManager {

  private static final Logger logger = LoggerFactory.getLogger(WriteIoContextManager.class);

  private List<BroadcastLog> newLogs;



  private List<BroadcastLog> logsToCreate;





  private List<BroadcastLog> logsToCreateForSkipCloneVolume = new ArrayList<>();
  private List<WriteIoContextManager> managersToCommit;
  private Map<IoMember, Broadcastlog.PbWriteResponse> writeResponseMap;
  private int countOfFinalStatusLog;
  private IoRequestType ioRequestType;
  private String className;



  public WriteIoContextManager(Long volumeId, SegId segId, IoUnitContextPacket callback,
      Coordinator coordinator,
      List<BroadcastLog> newLogs, IoRequestType ioRequestType) {
    super(volumeId, segId, callback, CoordinatorConfigSingleton.getInstance().isStreamIo(),
        coordinator);
    this.newLogs = newLogs;
    this.logsToCreate = newLogs;
    if (callback.getRequestType().isWrite()) {
      this.managersToCommit = coordinator.getUncommittedLogManager()
          .pollLogManagerToCommit(getVolumeId(), getSegId());
    } else {
      this.managersToCommit = new ArrayList<>();
    }
    this.ioRequestType = ioRequestType;
    this.writeResponseMap = new ConcurrentHashMap<>();


    this.className = "WriteIoContextManager";
  }

  private void done(boolean doneDirectly) {
    if (!allClonedSourceVolumeListenerHadDone()) {
      logger.error("request done {} at {}", getRequestId(), getSegId());
    }
    watchIfCostMoreTime(TraceAction.Write);
    logger.info("request done {} at {}", getRequestId(), getSegId());
    if (callback != null) {
      callback.complete();
      callback = null;
    }


    coordinator.getLogRecorder().removeCreateLogs(getSegId(), newLogs);
    // ready for committing the logs
    if (!doneDirectly) {
      // remove log which have not create successfully by primary
      Iterator<BroadcastLog> iterator = newLogs.iterator();
      while (iterator.hasNext()) {
        BroadcastLog broadcastLog = iterator.next();
        if (broadcastLog.getLogId() <= 0) {
          // not create successfully, do not need to commit it any more, remove it
          logger.info(
              "ori:{} can not create log:{} at:{} with members:{}, do not need to commit it any "
                  + "more",
              getOriRequestId(), broadcastLog.getLogUuid(), segId,
              getIoActionContext().getIoMembers());
          iterator.remove();
        }
      }
      if (!newLogs.isEmpty()) {
        if (this.ioRequestType.isWrite()) {
          cleanUnusedObjects();
          coordinator.getUncommittedLogManager().addLogManagerToCommit(this);
        }
        // else discard request, do nothing
      } else {
        logger.warn(
            "ori:{} all logs have not create successfully, do not need put it to uncommitted log "
                + "manager",
            getOriRequestId());
      }
    }

  }

  private void processCommitLogs() {
    // deal with managers which logs have been committed.
    List<WriteIoContextManager> leftManager = dealWithManagersToBeCommitted();

    // if there are still some managers to commit, we should put it back.
    if (leftManager.size() > 0) {
      coordinator.getUncommittedLogManager().addLogManagerToCommit(leftManager);
    }
    managersToCommit.clear();
  }

  private List<BroadcastLog> processLogsToCreate() {
    List<BroadcastLog> leftLogs = new ArrayList<>();
    List<BroadcastLog> successLogs = new ArrayList<>();
    logger.debug("io action context: {}, {}", getIoActionContext(), writeResponseMap.size());
    logsToCreateForSkipCloneVolume.clear();
    for (int i = 0; i < logsToCreate.size(); i++) {
      AtomicBoolean beNeedSkipFromSourceVolumeData = new AtomicBoolean(false);
      Pair<BroadcastLogStatus, Long> logStatusAndId = LogResult
          .mergeCreateResult(writeResponseMap, i, getIoActionContext(),
              coordinator.getVolumeType(getVolumeId()), beNeedSkipFromSourceVolumeData);
      BroadcastLog log = logsToCreate.get(i);
      log.setStatus(logStatusAndId.getFirst());
      if (logger.isDebugEnabled()) {
        logger.debug("log: {}", log);
      }

      logger.debug("ori:{} get the BroadcastLog status:{} at:{} members:{}", getRequestId(),
          log.getStatus(),
          getSegId(), getIoActionContext().getIoMembers());
      if (log.isCreateCompletion()) {
        log.setLogId(logStatusAndId.getSecond());
        Validate.isTrue(log.getLogId() > 0);


        log.release();
        successLogs.add(log);
      } else {
        if (beNeedSkipFromSourceVolumeData.get()) {
          logsToCreateForSkipCloneVolume.add(log);
        } else {
          leftLogs.add(log);
        }
      }
    }

    if (successLogs.size() > 0) {
      // add the logs that have been created in case read requests can not read the data.
      coordinator.getLogRecorder().addCreatedLogs(getSegId(), successLogs);
      for (BroadcastLog log : successLogs) {
        log.getIoContext().getIoUnit().setSuccess(log.isCreateSuccess());


        log.done();
      }
    }

    if (!leftLogs.isEmpty()) {
      logger.info("ori:{} still has:[{}] logs write failed at:{} members:{}", getRequestId(),
          leftLogs.size(),
          getSegId(), getIoActionContext().getIoMembers());
      LoggerTracer.getInstance()
          .mark(getRequestId(), className,
              "ori:{} still has:[{}] logs write failed at:{} members:{}",
              getRequestId(), leftLogs.size(), getSegId(), getIoActionContext().getIoMembers());
    }

    if (!logsToCreateForSkipCloneVolume.isEmpty()) {
      logger.info("ori:{} still has:[{}] logs write skipped at:{} members:{}", getRequestId(),
          leftLogs.size(),
          getSegId(), getIoActionContext().getIoMembers());
      LoggerTracer.getInstance()
          .mark(getRequestId(), className,
              "ori:{} still has:[{}] logs write skipped at:{} members:{}",
              getRequestId(), logsToCreateForSkipCloneVolume.size(), getSegId(),
              getIoActionContext().getIoMembers());
    }
    return leftLogs;
  }

  @Override
  public void doResultForLinkedCloneVolume(ClonedSourceVolumeReadListener listener) {
    reentrantLock.lock();
    try {
      listener.done();
      if (logsToCreate.size() == 0 && allClonedSourceVolumeListenerHadDone()) {

        doWhenAllLogsHaveBeenCreated();
        return;
      }

      if (isExpired()) {
        if (!allClonedSourceVolumeListenerHadDone()) {

          logger.info("some volume listener has not done");
          return;
        }


        expiredAfterResend();
        return;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  @Override
  public void doResult() {
    reentrantLock.lock();
    try {
      long responseInterval = getResponseInterval();
      // after all results has been collected, check the status of logs first.
      if (writeResponseMap != null && !writeResponseMap.isEmpty()) {
        // retry to create logs that are not created in current request.
        logsToCreate = processLogsToCreate();
      }

      if (logsToCreate.size() == 0 && logsToCreateForSkipCloneVolume.size() == 0
          && allClonedSourceVolumeListenerHadDone()) {

        doWhenAllLogsHaveBeenCreated();
        return;
      }

      if (isExpired()) {
        if (!allClonedSourceVolumeListenerHadDone()) {
          // wait clone task done, it should be expired soon.
          logger.info("some volume listener has not done");
          return;
        }


        expiredAfterResend();
        return;
      }

      // maybe all logs that need to be committed have done.
      processCommitLogs();

      if (streamIO) {
        SegmentForm segmentForm = getIoActionContext().getSegmentForm();
        Validate.notNull(segmentForm);

        boolean doneDirectly = false;
        if (this.doneDirectly.get()) {
          doneDirectly = true;
        } else if (segmentForm
            .readDoneDirectly(primaryDisconnectCount.get(), secondaryDisconnectCount.get(),
                joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(),
                coordinator.getVolumeType(getVolumeId()))) {
          doneDirectly = true;
        }

        if (doneDirectly) {
          doneDirectly();
          return;
        } else {
          logger.info(
              "ori:{} at:{} re-write, primary dis count:{}, secondary dis count:{}, joining "
                  + "secondary dis count:{}, arbiter dis count:{}, segment form:{}, done "
                  + "directly:{}",
              getRequestId(), segId, primaryDisconnectCount.get(),
              secondaryDisconnectCount.get(),
              joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(),
              segmentForm, doneDirectly);
          LoggerTracer.getInstance().mark(getRequestId(), className,
              "ori:{} at:{} re-write, primary dis count:{}, secondary dis count:{}, joining "
                  + "secondary dis count:{}, arbiter dis count:{}, segment form:{}, done "
                  + "directly:{}",
              getRequestId(), segId, primaryDisconnectCount.get(),
              secondaryDisconnectCount.get(),
              joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(),
              segmentForm, doneDirectly);
        }

        resetRequestFailedInfo();
      }

      if (logsToCreate.size() != 0) {
        logger
            .info("write io manager:{} still has {} logs didn't create success",
                getRequestId(),
                logsToCreate.size());
        boolean needUpdateMembership = needUpdateMembership(
            coordinator.getVolumeType(getVolumeId()).getWriteQuorumSize(),
            getIoActionContext().getIoMembers().size());
        logger.info(
            "write io manager:{} still has {} logs didn't create success, needUpdateMembership {}"
                + " resend",
            getRequestId(), logsToCreate.size(), needUpdateMembership);
        // submit to delay queue to re-send
        final ResendRequest resendRequest = new ResendRequest(this, needUpdateMembership,
            needDelay.get() ? DEFAULT_DELAY_MS : randomLittleDelayTime());

        resetNeedUpdateMembership();
        resetDelay();
        coordinator.getDelayManager().put(resendRequest);
      }


      if (logsToCreateForSkipCloneVolume.size() != 0) {
        logger
            .info("write io manager:{} still has {} logs need skip for clone source",
                getRequestId(),
                logsToCreateForSkipCloneVolume.size());



        Long sourceVolumeId = coordinator.getVolumeMetaData(volumeId).getSourceVolumeId();
        Integer sourceSnapshotId = coordinator.getVolumeMetaData(volumeId)
            .getSourceSnapshotId();


        List<IoUnitContext> ioUnitContexts = new ArrayList<>();
        Map<IoUnitContext, BroadcastLog> waitReadSourceVolumeLogs = new HashMap<>();
        for (BroadcastLog broadcastLog : logsToCreateForSkipCloneVolume) {
          int pageSize = CoordinatorConfigSingleton.getInstance().getPageSize();
          long offset = (broadcastLog.getOffset() / pageSize) * pageSize;
          IoUnit ioUnit = new ReadIoUnitImpl(segId.getIndex(),
              broadcastLog.getPageIndexInSegment(), offset, pageSize);
          IoUnitContext ioUnitContext = new SkipForSourceVolumeIoUnitContext(
              IoRequestType.Read, ioUnit, new SegId(sourceVolumeId, segId.getIndex()),
              broadcastLog.getPageIndexInSegment(),
              null);
          ioUnitContexts.add(ioUnitContext);

          waitReadSourceVolumeLogs.put(ioUnitContext, broadcastLog);
        }


        ClonedSourceVolumeListenerForWriteIO listener = new ClonedSourceVolumeListenerForWriteIO(
            this,
            waitReadSourceVolumeLogs);
        addClonedSourceVolumeListener(listener);


        IoUnitContextPacket ioUnitContextPacket = new SkipForSourceVolumeIoUnitContextPacket(
            sourceVolumeId, sourceSnapshotId, ioUnitContexts, segId.getIndex(),
            IoRequestType.Read, listener);


        ReadIoContextManager ioContextManager = new ReadIoContextManager(sourceVolumeId,
            new SegId(sourceVolumeId, segId.getIndex()), ioUnitContextPacket, coordinator);
        Set<Long> pageIndexes = new HashSet<>();
        for (IoUnitContext ioContext : ioUnitContexts) {
          pageIndexes.add(ioContext.getPageIndexInSegment());
        }
        ioContextManager.setRelatedPageIndexes(pageIndexes);
        ioContextManager.setExpiredTime(this.getExpiredTime());
        listener.setReadIoContextManager(ioContextManager);


        final ResendRequest resendRequest = new ResendRequest(ioContextManager, false,
            0);

        resetNeedUpdateMembership();
        resetDelay();
        LoggerTracer.getInstance()
            .mark(this.getRequestId(), className,
                "ori:{} begin read source data for partial unit write. source ori:{}",
                ioContextManager.getRequestId(),
                this.getRequestId());
        coordinator.getDelayManager().put(resendRequest);
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  @Override
  public void doneDirectly() {
    if (!allClonedSourceVolumeListenerHadDone()) {
      logger.error("found some exception with some listener has not done, ori:{}",
          getRequestId(), new Exception());
      return;
    }
    // prepare write logs
    List<BroadcastLog> allLogs = new ArrayList<>(logsToCreate);
    allLogs.addAll(logsToCreateForSkipCloneVolume);
    logger.info("ori:{} write done directly at logic segment:{}", getRequestId(),
        getLogicalSegmentIndex());
    for (int i = 0; i < allLogs.size(); i++) {
      BroadcastLog log = allLogs.get(i);
      log.release();
      log.getIoContext().getIoUnit().setSuccess(true);
      // notify the NBD client successfully, there is no need waiting for the logs to be committed.
      log.done();
    }

    // releaseBody memory as soon as possible.
    setRequestBuilder(null);


    done(true);
  }

  private void doWhenAllLogsHaveBeenCreated() {
    processCommitLogs();


    setRequestBuilder(null);
    done(false);
  }

  private void expiredAfterResend() {
    logger.error("ori:{} write is EXPIRED", getRequestId());
    processCommitLogs();

    // releaseBody memory as soon as possible.
    setRequestBuilder(null);

    List<BroadcastLog> allLogs = new ArrayList<>(logsToCreate);
    allLogs.addAll(logsToCreateForSkipCloneVolume);

    for (BroadcastLog log : allLogs) {

      log.release();
      Validate.isTrue(!log.isCreateCompletion());
      if (streamIO) {
        log.setStatus(BroadcastLogStatus.Created);
        log.getIoContext().getIoUnit().setSuccess(true);
      } else {
        log.setStatus(BroadcastLogStatus.Abort);
        log.getIoContext().getIoUnit().setSuccess(false);
      }
      log.done();
    }

    allLogs.clear();
    logsToCreateForSkipCloneVolume.clear();
    logsToCreate.clear();


    done(false);
  }


  private List<WriteIoContextManager> dealWithManagersToBeCommitted() {

    List<WriteIoContextManager> leftManagers = new ArrayList<>();

    for (int managerToCommitIndex = 0; managerToCommitIndex < managersToCommit.size();
        managerToCommitIndex++) {
      WriteIoContextManager managerToCommit = managersToCommit.get(managerToCommitIndex);
      LogResult.mergeCommitResult(writeResponseMap, managerToCommitIndex, managerToCommit,
          getIoActionContext(),
          coordinator.getVolumeType(getVolumeId()));
      if (managerToCommit.isAllFinalStatus()) {


        managerToCommit.releaseReference();
      } else {
        leftManagers.add(managerToCommit);
      }
    }

    return leftManagers;
  }



  public void commitPbLog(int index, PbBroadcastLog pbLog) {
    BroadcastLog newLog = newLogs.get(index);
    Validate.isTrue(pbLog.getLogId() == newLog.getLogId());
    boolean isFinalStatus = newLog.isFinalStatus();

    BroadcastLogStatus newStatus = PbRequestResponseHelper
        .convertPbStatusToStatus(pbLog.getLogStatus());
    if (PbRequestResponseHelper.isFinalStatus(newStatus)) {
      newLog.setStatus(newStatus);
      if (!isFinalStatus) {
        countOfFinalStatusLog++;
      }
    }
  }



  @Deprecated
  public void commitThriftLog(int index, BroadcastLogThrift thriftLog) {
    BroadcastLog newLog = newLogs.get(index);
    Validate.isTrue(thriftLog.getLogId() == newLog.getLogId());
    boolean isFinalStatus = newLog.isFinalStatus();

    BroadcastLogStatus newStatus = RequestResponseHelper
        .convertThriftStatusToStatus(thriftLog.getLogStatus());
    if (PbRequestResponseHelper.isFinalStatus(newStatus)) {
      newLog.setStatus(newStatus);
      if (!isFinalStatus) {
        countOfFinalStatusLog++;
      }
    }
  }

  public boolean isAllFinalStatus() {
    return countOfFinalStatusLog == newLogs.size();
  }

  public List<BroadcastLog> getLogsToCreate() {
    return this.logsToCreate;
  }

  public List<BroadcastLog> getLogsToCommit() {
    return this.newLogs;
  }

  public Coordinator getCoordinator() {
    return coordinator;
  }

  public List<WriteIoContextManager> getManagersToCommit() {
    return managersToCommit;
  }


  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {

    Set<Long> oldUuids = new HashSet<>();
    Set<Long> newUuids = new HashSet<>();
    for (BroadcastLog log : logsToCreate) {
      oldUuids.add(log.getLogUuid());
      Long newUuid = coordinator.generateLogUuid();
      log.setLogUuid(newUuid);
      newUuids.add(newUuid);
    }
    logger.info(
        "ori:{} replace old log Uuids:{} with new log Uuids:{} for new temp primary at segId:{}",
        getOriRequestId(), oldUuids, newUuids, getSegId());
    LoggerTracer.getInstance().mark(getRequestId(), className,
        "ori:{} replace old log Uuids:{} with new log Uuids:{} for new temp primary at segId:{}",
        getOriRequestId(), oldUuids, newUuids, getSegId());
  }

  @Override
  public void initRequestCount(int requestCount) {
    this.writeResponseMap.clear();
    super.initRequestCount(requestCount);
  }

  @Override
  public void releaseReference() {
    super.releaseReference();
    this.newLogs.clear();
    this.newLogs = null;
  }

  @Override
  public void processResponse(Object object, IoMember ioMember) {
    Validate.notNull(ioMember);
    if (object != null) {
      this.writeResponseMap.put(ioMember, (Broadcastlog.PbWriteResponse) object);
    }
    int requestCount = super.requestCount.decrementAndGet();
    if (requestCount != 0) {
      return;
    }
    doResult();
  }

  @Override
  protected void cleanUnusedObjects() {
    super.cleanUnusedObjects();


    // logsToCreate
    logsToCreate.clear();
    logsToCreate = null;

    // managersToCommit
    managersToCommit.clear();
    managersToCommit = null;

    // writeResponseMap
    writeResponseMap.clear();
    writeResponseMap = null;
  }
}
