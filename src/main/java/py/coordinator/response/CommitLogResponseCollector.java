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

package py.coordinator.response;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.TraceAction;
import py.coordinator.lib.Coordinator;
import py.coordinator.logmanager.AbstractIoContextManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.volume.VolumeType;


public class CommitLogResponseCollector implements TriggerByCheckCallback {

  public static final int DEBUG_COMMIT_LOG_TIMEOUT_MS_THRESHOLD = 30000;
  private static final Logger logger = LoggerFactory.getLogger(CommitLogResponseCollector.class);
  protected final IoActionContext ioActionContext;
  protected final AtomicBoolean doneDirectly;
  protected final AtomicInteger primaryDisconnectCount;
  protected final AtomicInteger secondaryDisconnectCount;
  protected final AtomicInteger joiningSecondaryDisconnectCount;
  protected final AtomicInteger arbiterDisconnectCount;
  protected final boolean streamIO;
  protected final boolean lastRound;
  private final Coordinator coordinator;
  private final List<WriteIoContextManager> managers;
  private final AtomicInteger needUpdateMembership;
  public SegmentMembership membershipWhenIoCome;
  protected Set<WriteIoContextManager> needRedriveManager;
  private Long requestId;
  private VolumeType volumeType;
  private Long volumeId;
  private SegId segId;
  private CommitLogCallback[] callbacks;
  private AtomicInteger callbackCounter;
  private boolean canCommit;
  private String className;



  public CommitLogResponseCollector(Long requestId, SegmentMembership membershipWhenIoCome,
      Long volumeId,
      SegId segId, Coordinator coordinator, List<WriteIoContextManager> managers,
      CommitLogCallback[] callbacks,
      AtomicInteger callbackCounter, IoActionContext ioActionContext, boolean streamIO,
      boolean lastRound) {
    this.coordinator = coordinator;
    this.requestId = requestId;
    this.managers = managers;
    this.membershipWhenIoCome = membershipWhenIoCome;
    this.volumeId = volumeId;
    this.volumeType = this.coordinator.getVolumeType(volumeId);
    this.callbacks = callbacks;
    this.segId = segId;
    this.callbackCounter = callbackCounter;
    this.ioActionContext = ioActionContext;
    this.needUpdateMembership = new AtomicInteger(0);
    this.doneDirectly = new AtomicBoolean(false);
    this.primaryDisconnectCount = new AtomicInteger(0);
    this.secondaryDisconnectCount = new AtomicInteger(0);
    this.joiningSecondaryDisconnectCount = new AtomicInteger(0);
    this.arbiterDisconnectCount = new AtomicInteger(0);
    this.streamIO = streamIO;
    this.lastRound = lastRound;
    this.canCommit = false;

    this.className = "CommitLogResponseCollector";
  }



  public boolean canDoResult() {
    if (callbackCounter.decrementAndGet() == 0) {
      return true;
    }
    return false;
  }



  public boolean needUpdateMembership() {
    if (this.needUpdateMembership.get() >= volumeType.getVotingQuorumSize()
        || this.needUpdateMembership.get() >= ioActionContext.getIoMembers().size()) {
      return true;
    }
    return false;
  }



  public Set<WriteIoContextManager> processResponses() {
    Set<WriteIoContextManager> needRedriveManager = new HashSet<>();

    for (int managerToCommitIndex = 0; managerToCommitIndex < managers.size();
        managerToCommitIndex++) {
      WriteIoContextManager ioContextManager = managers.get(managerToCommitIndex);
      for (int logIndexInOneManager = 0;
          logIndexInOneManager < ioContextManager.getLogsToCommit().size();
          logIndexInOneManager++) {

        int goodPrimaryCount = 0;
        int goodSecondariesCount = 0;
        int goodJoiningSecondariesCount = 0;

        Broadcastlog.PbBroadcastLog pbBroadcastLog = null;
        for (int i = 0; i < callbacks.length; i++) {
          if (callbacks[i] == null || !callbacks[i].hasResponse()) {
            continue;
          }

          if (callbacks[i].isError()) {
            continue;
          }

          // commit log from whichever response can be accept
          pbBroadcastLog = callbacks[i].getResponse().getLogManagersToCommit(managerToCommitIndex)
              .getBroadcastLogs(logIndexInOneManager);
          if (callbacks[i].getMemberIoStatus().isPrimary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbBroadcastLog.getLogStatus())) {
              goodPrimaryCount++;
            }
          } else if (callbacks[i].getMemberIoStatus().isSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbBroadcastLog.getLogStatus())) {
              goodSecondariesCount++;
            }
          } else if (callbacks[i].getMemberIoStatus().isJoiningSecondary()) {
            if (PbRequestResponseHelper.isFinalStatus(pbBroadcastLog.getLogStatus())) {
              goodJoiningSecondariesCount++;
            }
          } else {
            logger.error("unknown write member:{}, {} at:{}", i, callbacks[i].getMemberIoStatus(),
                segId);
            Validate.isTrue(false);
          }
        }

        SegmentForm segmentForm = ioActionContext.getSegmentForm();

        canCommit = segmentForm
            .mergeCommitLogResult(goodPrimaryCount, goodSecondariesCount,
                goodJoiningSecondariesCount,
                ioActionContext, volumeType);

        if (canCommit) {
          if (pbBroadcastLog == null) {
            Validate.isTrue(false,
                "something goes wrong at:" + segId + " ,now segment form:" + segmentForm);
          }

          ioContextManager.commitPbLog(logIndexInOneManager, pbBroadcastLog);
        }
      }

      if (ioContextManager.isAllFinalStatus()) {

        ioContextManager.releaseReference();
      } else {
        needRedriveManager.add(ioContextManager);
      }
    }
    if (streamIO) {

      boolean noNeedDrive;
      SegmentForm segmentForm = ioActionContext.getSegmentForm();
      if (doneDirectly.get()) {
        noNeedDrive = true;
      } else {
        noNeedDrive = segmentForm
            .writeDoneDirectly(primaryDisconnectCount.get(), secondaryDisconnectCount.get(),
                joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(), volumeType);
      }
      if (noNeedDrive) {
        logger.warn("no need to driver ori:{} at:{} any more", getOriRequestId(), segId);
        needRedriveManager.clear();
      } else {
        LoggerTracer.getInstance().mark(getRequestId(), className,
            "ori:{} at:{} re-write, primary dis count:{}, secondary dis count:{}, joining "
                + "secondary dis count:{}, arbiter dis count:{}, segment form:{}, done directly:{}",
            getRequestId(), segId, primaryDisconnectCount.get(), secondaryDisconnectCount.get(),
            joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(), segmentForm,
            doneDirectly);
      }
    }

    return needRedriveManager;
  }

  public Long getRequestId() {
    return requestId;
  }

  @Override
  public long getOriRequestId() {
    return getRequestId();
  }

  @Override
  public void triggeredByCheckCallback() {

  }

  @Override
  public void markNeedUpdateMembership() {
    this.needUpdateMembership.incrementAndGet();
  }

  @Override
  public void noNeedUpdateMembership() {
    this.needUpdateMembership.set(AbstractIoContextManager.NO_NEED_UPDATE_MEMBERSHIP_FLAG);
  }

  @Override
  public void resetNeedUpdateMembership() {
    this.needUpdateMembership.set(0);
  }

  @Override
  public void markDoneDirectly() {
    this.doneDirectly.set(true);
  }

  @Override
  public void markRequestFailed(InstanceId whoIsDisconnect) {
    MemberIoStatus memberIoStatus = membershipWhenIoCome.getMemberIoStatus(whoIsDisconnect);

    if (memberIoStatus.isPrimary()) {
      primaryDisconnectCount.incrementAndGet();
    } else if (memberIoStatus.isSecondary()) {
      secondaryDisconnectCount.incrementAndGet();
    } else if (memberIoStatus.isJoiningSecondary()) {
      joiningSecondaryDisconnectCount.incrementAndGet();
    } else if (memberIoStatus.isArbiter()) {
      arbiterDisconnectCount.incrementAndGet();
    } else {
      Validate.isTrue(false,
          "segment membership:" + membershipWhenIoCome + ", disconnect:" + whoIsDisconnect);
    }
  }

  @Override
  public void resetRequestFailedInfo() {
    doneDirectly.set(false);
    primaryDisconnectCount.set(0);
    secondaryDisconnectCount.set(0);
    joiningSecondaryDisconnectCount.set(0);
    arbiterDisconnectCount.set(0);
  }

  @Override
  public boolean streamIO() {
    return streamIO;
  }

  @Override
  public void doneForCommitLog() {
    if (needRedriveManager != null && !needRedriveManager.isEmpty()) {
      coordinator.getUncommittedLogManager().addLogManagerToCommit(needRedriveManager);
    } else {

      coordinator.getCommitLogWorker().put(volumeId, segId, !canCommit);
    }


    LoggerTracer.getInstance()
        .doneTrace(getOriRequestId(), TraceAction.CommitLog, DEBUG_COMMIT_LOG_TIMEOUT_MS_THRESHOLD);
  }

  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {

  }

  @Override
  public Long getVolumeId() {
    return volumeId;
  }

}
