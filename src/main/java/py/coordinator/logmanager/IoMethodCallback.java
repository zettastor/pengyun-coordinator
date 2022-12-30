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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.LoggerTracer;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.Coordinator;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.response.TriggerByCheckCallback;
import py.coordinator.utils.NetworkDelayRecorder;
import py.instance.InstanceId;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;
import py.netty.core.AbstractMethodCallback;
import py.netty.exception.DisconnectionException;
import py.netty.exception.NetworkUnhealthyException;
import py.netty.exception.SegmentDeleteException;
import py.netty.exception.SegmentNotFoundException;


public class IoMethodCallback<T> extends AbstractMethodCallback<T> implements
    TriggerByCheckCallback {

  private static final Logger logger = LoggerFactory.getLogger(IoMethodCallback.class);
  private final ContinueProcessor continueProcessor;
  private final AtomicInteger counter;

  private final NbdRequestType requestType;
  protected IoContextManagerForAsyncDatanodeCallback ioContextManager;
  private Exception exception;
  private IoMember ioMember;
  private SegmentMembership membershipWhenIoCome;
  private long startTimeMs;
  private NetworkDelayRecorder networkDelayRecorder;



  public IoMethodCallback(IoContextManagerForAsyncDatanodeCallback ioContextManager,
      AtomicInteger counter, Coordinator coordinator,
      IoMember ioMember) {
    Validate.notNull(ioMember);
    this.counter = counter;
    this.ioContextManager = ioContextManager;
    this.ioMember = ioMember;
    this.membershipWhenIoCome = ioContextManager.getIoActionContext().getMembershipWhenIoCome();
    if (ioContextManager instanceof WriteIoContextManager) {
      this.requestType = NbdRequestType.Write;
    } else {
      this.requestType = NbdRequestType.Read;
    }
    this.startTimeMs = System.currentTimeMillis();
    this.networkDelayRecorder = coordinator.getNetworkDelayRecorder();
    continueProcessor = new ContinueProcessor(coordinator, getVolumeId(),
        ioContextManager.getSegId(), getOriRequestId(),
        ioContextManager.getRequestId(), ioMember, membershipWhenIoCome, this, ioContextManager);

  }


  public String getClassName() {
    throw new NotImplementedException("");
  }

  private void recordNetworkCostTime() {
    long networkCostTimeMs = System.currentTimeMillis() - startTimeMs;
    networkDelayRecorder.recordDelay(this.ioMember.getEndPoint().getHostName(), networkCostTimeMs);
  }

  @Override
  public void complete(T msg) {
    recordNetworkCostTime();



    int counter = this.counter.decrementAndGet();
    if (counter != 0) {
      if (counter < 0) {
        Validate.isTrue(false,
            "ori:" + ioContextManager.getRequestId() + ", counter can not be:" + counter);
      }
      if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
        LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
            "ori:{} successfully {} to endPoint:{} at:{}", ioContextManager.getRequestId(),
            requestType,
            ioMember.getEndPoint(), ioContextManager.getSegId());
      }
      if (logger.isInfoEnabled()) {
        logger.debug("ori:{} successfully {} to endPoint:{} at:{}", ioContextManager.getRequestId(),
            requestType,
            ioMember.getEndPoint(), ioContextManager.getSegId());
      }
    }


    if (logger.isInfoEnabled()) {
      logger.debug("ori:{} finally successfully {} to endPoint:{}, at:{}, io member count:{}",
          ioContextManager.getRequestId(), requestType, ioMember.getEndPoint(),
          ioContextManager.getSegId(),
          ioContextManager.getIoActionContext().getIoMembers().size());
    }
    if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
          "ori:{} finally successfully {} to endPoint:{}, at:{}, io member count:{}",
          ioContextManager.getRequestId(), requestType, ioMember.getEndPoint(),
          ioContextManager.getSegId(),
          ioContextManager.getIoActionContext().getIoMembers().size());
    }

    try {
      Validate.notNull(msg);
      ioContextManager.processResponse(msg, ioMember);
    } catch (Exception e) {
      logger
          .error("{} request:{} caught an exception", requestType, ioContextManager.getRequestId(),
              e);
    }
  }

  @Override
  public void fail(Exception e) {
    recordNetworkCostTime();
    this.exception = e;
    boolean continueProcess = true;
    try {
      continueProcess = continueProcessor.processException(exception, getClassName());
    } catch (Exception e1) {
      logger
          .error("ori:{} can not parse the {} request:{} exception", getOriRequestId(), requestType,
              ioContextManager.getRequestId(), e1);
    }

    if (needUpdateMembership()) {
      markNeedUpdateMembership();
    }

    if (!continueProcess) {
      ioContextManager.markRequestFailed(ioMember.getInstanceId());
      logger.info("ori:{} request:{}, endpoint:{} will be triggered by check callback at:{}",
          ioContextManager.getRequestId(), requestType, ioMember.getEndPoint(),
          ioContextManager.getSegId());
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
          "ori:{} request:{}, endpoint:{} will be triggered by check callback at:{}",
          ioContextManager.getRequestId(), requestType, ioMember.getEndPoint(),
          ioContextManager.getSegId());

      return;
    }

    failProcess();
  }

  private void failProcess() {
    int counter = this.counter.decrementAndGet();
    if (counter != 0) {
      if (counter < 0) {
        Validate.isTrue(false,
            "ori:" + ioContextManager.getRequestId() + ", counter can not be:" + counter);
      }
      LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
          "ori:{} {} request to:{} at:{} caught an exception:{}", getOriRequestId(), requestType,
          ioMember.getEndPoint(), ioContextManager.getSegId(), exception.toString());
      logger.warn("ori:{} {} request to:{} at:{} caught an exception:{}", getOriRequestId(),
          requestType,
          ioMember.getEndPoint(), ioContextManager.getSegId(), exception.toString());
    }

    logger.info("failProcess ori:{} {} request to:{} at:{} the exception:{}, io member count:{}",
        getOriRequestId(),
        requestType, ioMember.getEndPoint(), ioContextManager.getSegId(), exception.toString(),
        ioContextManager.getIoActionContext().getIoMembers().size());
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
        "failProcess ori:{} {} request to:{} at:{} the exception:{}, io member count:{}",
        getOriRequestId(),
        requestType, ioMember.getEndPoint(), ioContextManager.getSegId(), exception.toString(),
        ioContextManager.getIoActionContext().getIoMembers().size());
    try {
      ioContextManager.processResponse(null, ioMember);
    } catch (Exception exception) {
      logger.error("ori:{} {} request from:{} at:{} caught an exception", getOriRequestId(),
          requestType,
          ioMember.getEndPoint(), ioContextManager.getSegId(), exception);
    }
  }

  @Override
  public long getOriRequestId() {
    return ioContextManager.getRequestId();
  }


  public void triggeredByCheckCallback() {
    logger.info("ori:{} request:{} at:{} {} trigger at check callback", getOriRequestId(),
        requestType,
        ioContextManager.getSegId(), ioMember.getEndPoint());
    LoggerTracer.getInstance().mark(ioContextManager.getRequestId(), getClassName(),
        "ori:{} request:{} at:{} {} trigger at check callback", getOriRequestId(), requestType,
        ioContextManager.getSegId(), ioMember.getEndPoint());
    failProcess();
  }

  @Override
  public void markNeedUpdateMembership() {
    ioContextManager.markNeedUpdateMembership();
  }

  @Override
  public void noNeedUpdateMembership() {
    ioContextManager.noNeedUpdateMembership();
  }

  @Override
  public void resetNeedUpdateMembership() {
    ioContextManager.resetNeedUpdateMembership();
  }

  @Override
  public void markDoneDirectly() {
    ioContextManager.markDoneDirectly();
  }

  @Override
  public void markRequestFailed(InstanceId whoIsDisconnect) {
    ioContextManager.markRequestFailed(whoIsDisconnect);
  }

  @Override
  public void resetRequestFailedInfo() {
    throw new NotImplementedException("not implement here");
  }

  @Override
  public boolean streamIO() {
    return ioContextManager.streamIO();
  }

  @Override
  public void doneForCommitLog() {
    throw new NotImplementedException("not implement here");
  }

  public T getResponse() {
    throw new NotImplementedException("not implement");
  }


  private boolean needUpdateMembership() {
    boolean needUpdateMembership = false;
    if (exception == null) {
      return needUpdateMembership;
    }
    if (exception instanceof SegmentNotFoundException || exception instanceof DisconnectionException
        || exception instanceof NetworkUnhealthyException
        || exception instanceof SegmentDeleteException) {
      needUpdateMembership = true;
    }
    return needUpdateMembership;
  }

  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {
    ioContextManager.replaceLogUuidForNotCreateCompletelyLogs();
  }

  @Override
  public Long getVolumeId() {
    return ioContextManager.getVolumeId();
  }

  public MemberIoStatus getMemberIoStatus() {
    return ioMember.getMemberIoStatus();
  }

  public IoMember getIoMember() {
    return this.ioMember;
  }

  public boolean weakGoodResponse() {
    throw new NotImplementedException("not implement here");
  }

  public Exception getException() {
    return exception;
  }

  public int getCounter() {
    return counter.get();
  }

}
