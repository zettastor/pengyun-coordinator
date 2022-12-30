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

import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.lib.Coordinator;
import py.coordinator.logmanager.ContinueProcessor;
import py.coordinator.worker.CommitWorkProgress;
import py.instance.InstanceId;
import py.instance.PortType;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.netty.core.MethodCallback;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.proto.Broadcastlog;
import py.proto.Commitlog;


public class CommitLogCallback implements MethodCallback<Commitlog.PbCommitlogResponse>,
    TriggerByCheckCallback {

  private static final Logger logger = LoggerFactory.getLogger(CommitLogCallback.class);
  private final Long volumeId;
  private final SegId segId;
  private final ContinueProcessor continueProcessor;
  private final Coordinator coordinator;
  private final CommitLogResponseCollector responseCollector;
  private final IoMember ioMember;
  private volatile Commitlog.PbCommitlogResponse response;
  private Exception exception;
  private Map<SegId, AtomicReference<CommitWorkProgress>> taskRecorder;
  private String className;

  public CommitLogCallback(Coordinator coordinator, Long volumeId, SegId segId,
      CommitLogResponseCollector responseCollector,
      IoMember ioMember, Map<SegId, AtomicReference<CommitWorkProgress>> taskRecorder) {
    this.volumeId = volumeId;
    this.segId = segId;
    this.coordinator = coordinator;
    this.responseCollector = responseCollector;
    this.ioMember = ioMember;
    this.exception = null;
    this.response = null;
    this.taskRecorder = taskRecorder;
    this.className = "CommitLogCallback";
    continueProcessor = new ContinueProcessor(coordinator, getVolumeId(), segId, getOriRequestId(),
        getOriRequestId(), ioMember, responseCollector.membershipWhenIoCome, this, null);
  }

 
  @Override
  public void complete(Commitlog.PbCommitlogResponse object) {
    try {
      response = object;
    } catch (Exception e) {
      exception = e;
      LoggerTracer.getInstance().mark(getOriRequestId(), className,
          "ori:{} [{}] told me that my membership has changed at segId:{}", getOriRequestId(),
          ioMember.getEndPoint(), segId, e);
      logger.error("ori:{} failed to commit log at:{}, @:{}", getOriRequestId(),
          ioMember.getEndPoint(), segId, e);
    } finally {
      try {
        if (needUpdateMembership()) {
          markNeedUpdateMembership();
        }
        processResponse();
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
    }
  }

  @Override
  public void fail(Exception e) {
    logger.warn("ori:{} caught an exception:{} when committing logs at segId:{} from:{}",
        getOriRequestId(),
        e.getClass().getSimpleName(), segId, ioMember.getEndPoint());
    exception = e;
    boolean continueProcess = true;

    try {
      continueProcess = continueProcessor.processException(exception, className);
    } catch (Exception e1) {
      logger.error("ori:{} can not parse the {} exception", getOriRequestId(), e1);
    }

    if (needUpdateMembership()) {
      markNeedUpdateMembership();
    }

    if (!continueProcess) {
      logger.info("ori:{} commit log work try to check {} at:{} reachable, so won't process here",
          getOriRequestId(), ioMember.getInstanceId(), segId);
      LoggerTracer.getInstance().mark(getOriRequestId(), className,
          "ori:{} commit log work try to check {} at:{} reachable, so won't process here",
          getOriRequestId(),
          ioMember.getInstanceId(), segId);
      return;
    }

    processResponse();
  }

  public boolean hasResponse() {
    return response != null || exception != null;
  }

  public Commitlog.PbCommitlogResponse getResponse() {
    return response;
  }

  public boolean isError() {
    return exception != null;
  }

  private void processResponse() {

    if (!responseCollector.canDoResult()) {
      return;
    }

    try {
      responseCollector.needRedriveManager = responseCollector.processResponses();
    } catch (Exception e) {
      logger.error("caught an exception when process commit responses", e);
    }

    if (responseCollector.needUpdateMembership()) {
      List<EndPoint> endpoints = RequestResponseHelper
          .buildEndPoints(coordinator.getInstanceStore(), responseCollector.membershipWhenIoCome,
              PortType.IO,
              true, null);
      logger.info(
          "ori:{} at:{} committing logs need update membership from datanodes:{}, current "
              + "membership:{}, io action:{}",
          getOriRequestId(), segId, endpoints, responseCollector.membershipWhenIoCome,
          responseCollector.ioActionContext);
      LoggerTracer.getInstance().mark(getOriRequestId(), className,
          "ori:{} at:{} committing logs need update membership from datanodes:{}, current "
              + "membership:{}, io action:{}",
          getOriRequestId(), segId, endpoints, responseCollector.membershipWhenIoCome,
          responseCollector.ioActionContext);
     
      GetMembershipCallbackForCommitLog getMembershipCallbackForCommitLog =
          new GetMembershipCallbackForCommitLog(
              coordinator, endpoints.size(), segId, getOriRequestId(), this);
      Broadcastlog.PbGetMembershipRequest.Builder builder = Broadcastlog.PbGetMembershipRequest
          .newBuilder();
      builder.setRequestId(RequestIdBuilder.get());
      builder.setVolumeId(segId.getVolumeId().getId());
      builder.setSegIndex(segId.getIndex());
      Broadcastlog.PbGetMembershipRequest request = builder.build();
      for (EndPoint endPoint : endpoints) {
        coordinator.getClientFactory().generate(endPoint)
            .getMembership(request, getMembershipCallbackForCommitLog);
      }
    } else {
      doneForCommitLog();
    }
  }

  public MemberIoStatus getMemberIoStatus() {
    return this.ioMember.getMemberIoStatus();
  }

  @Override
  public long getOriRequestId() {
    return responseCollector.getRequestId();
  }

  @Override
  public void triggeredByCheckCallback() {
    logger.info("ori:{} commit log callback trigger at:{}", getOriRequestId(), segId);
    LoggerTracer.getInstance()
        .mark(getOriRequestId(), className, "ori:{} commit log callback trigger at:{}",
            getOriRequestId(),
            segId);
    processResponse();
  }

  @Override
  public void markNeedUpdateMembership() {
    responseCollector.markNeedUpdateMembership();
  }

  @Override
  public void noNeedUpdateMembership() {
    responseCollector.noNeedUpdateMembership();
  }

  @Override
  public void resetNeedUpdateMembership() {
    responseCollector.resetNeedUpdateMembership();
  }

  @Override
  public void markDoneDirectly() {
    responseCollector.markDoneDirectly();
  }

  @Override
  public void markRequestFailed(InstanceId whoIsDisconnect) {
    responseCollector.markRequestFailed(whoIsDisconnect);
  }

  
  @Override
  public void resetRequestFailedInfo() {
    responseCollector.resetRequestFailedInfo();
  }

  @Override
  public boolean streamIO() {
    return responseCollector.streamIO();
  }

  @Override
  public void doneForCommitLog() {
    
    if (responseCollector.lastRound) {
      taskRecorder.get(segId).getAndSet(CommitWorkProgress.Commitable);
    }
    responseCollector.doneForCommitLog();
  }

  public boolean needUpdateMembership() {
    if (this.exception != null) {
      return true;
    }

    return false;
  }

  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {
   
  }

  @Override
  public Long getVolumeId() {
    return this.volumeId;
  }


  @Override
  public ByteBufAllocator getAllocator() {
    return PooledByteBufAllocatorWrapper.INSTANCE;
  }
}
