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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.TraceAction;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.lib.Coordinator;
import py.coordinator.pbrequest.RequestBuilder;
import py.coordinator.response.TriggerByCheckCallback;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;

/**
 * xx.
 */
public abstract class AbstractIoContextManager implements IoContextManager,
    IoContextManagerForAsyncDatanodeCallback, TriggerByCheckCallback {

  public static final int NO_NEED_UPDATE_MEMBERSHIP_FLAG = -10000;
  public static final int DEFAULT_DELAY_MS = 1000;
  public static final int LITTLE_DELAY_THRESHOLD_MS = 20;
  protected static final Logger logger = LoggerFactory.getLogger(AbstractIoContextManager.class);
  private static final AtomicLong requestIdGenerator = new AtomicLong(0);
  protected final SegId segId;
  protected final Long volumeId;
  protected final int logicalSegIndex;
  protected final AtomicBoolean doneDirectly;
  protected final AtomicInteger primaryDisconnectCount;
  protected final AtomicInteger secondaryDisconnectCount;
  protected final AtomicInteger joiningSecondaryDisconnectCount;
  protected final AtomicInteger arbiterDisconnectCount;
  protected final boolean streamIO;
  protected final AtomicBoolean needDelay;
  protected final ReentrantLock reentrantLock = new ReentrantLock();
 
  private final long requestId;
  private final AtomicInteger needUpdateMembershipCount;
  private final long startIoTime;
  private final AtomicLong primaryRspTimestamp;
  private final AtomicLong secondaryRspTimestamp;
  public AtomicInteger requestCount;
  protected IoUnitContextPacket callback;
  protected RequestBuilder<?> requestBuilder;
  protected Coordinator coordinator;
  private long expiredTime;
  private int retryTimes;
  private IoActionContext ioActionContext;
  private List<ClonedSourceVolumeReadListener> clonedSourceVolumeReadListeners = new ArrayList<>();


  
  public AbstractIoContextManager(Long volumeId, SegId segId, IoUnitContextPacket callback,
      boolean streamIO, Coordinator coordinator) {
    this.volumeId = volumeId;
    this.callback = callback;
    this.logicalSegIndex = callback.getLogicalSegIndex();
    this.coordinator = coordinator;
    this.retryTimes = 0;
    this.expiredTime = System.currentTimeMillis();
    this.segId = segId;
    this.requestId = requestIdGenerator.getAndIncrement();
    this.needUpdateMembershipCount = new AtomicInteger(0);
    this.startIoTime = System.currentTimeMillis();
    this.doneDirectly = new AtomicBoolean(false);
    this.primaryDisconnectCount = new AtomicInteger(0);
    this.secondaryDisconnectCount = new AtomicInteger(0);
    this.joiningSecondaryDisconnectCount = new AtomicInteger(0);
    this.arbiterDisconnectCount = new AtomicInteger(0);
    this.streamIO = streamIO;
    this.needDelay = new AtomicBoolean(false);
    this.primaryRspTimestamp = new AtomicLong(0);
    this.secondaryRspTimestamp = new AtomicLong(0);
    this.requestCount = new AtomicInteger(0);
  }

  @Override
  public long getRequestId() {
    return requestId;
  }

  @Override
  public int getLogicalSegmentIndex() {
    return this.logicalSegIndex;
  }

  @Override
  public SegId getSegId() {
    return segId;
  }

  @Override
  public RequestBuilder<?> getRequestBuilder() {
    return requestBuilder;
  }

  @Override
  public void setRequestBuilder(RequestBuilder<?> requestBuilder) {
    this.requestBuilder = requestBuilder;
  }

  protected void addClonedSourceVolumeListener(ClonedSourceVolumeReadListener listener) {
    clonedSourceVolumeReadListeners.add(listener);
  }

  protected boolean allClonedSourceVolumeListenerHadDone() {
    return clonedSourceVolumeReadListeners.stream().allMatch(listener -> {
      return listener.isDone();
    });
  }

  @Override
  public long getExpiredTime() {
    return this.expiredTime;
  }

  @Override
  public void setExpiredTime(long expiredTime) {
    this.expiredTime = expiredTime;
  }

  @Override
  public boolean isExpired() {
    if (System.currentTimeMillis() > expiredTime) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public IoUnitContextPacket getCallback() {
    return callback;
  }

  @Override
  public int incFailTimes() {
    return ++retryTimes;
  }

  @Override
  public int getFailTimes() {
    return retryTimes;
  }

  @Override
  public boolean needUpdateMembership(int quorumSize, int ioMemberCount) {
    boolean needUpdate = false;
    if (needUpdateMembershipCount.get() >= quorumSize
        || needUpdateMembershipCount.get() >= ioMemberCount) {
      logger.info("ori:{} need to update membership at:{}, ioMember count:{} action context:{}",
          requestId, segId,
          ioMemberCount, ioActionContext);
      needUpdate = true;
    }
    return needUpdate;
  }

  @Override
  public void resetNeedUpdateMembership() {
    this.needUpdateMembershipCount.set(0);
  }

  @Override
  public void markNeedUpdateMembership() {
    this.needUpdateMembershipCount.incrementAndGet();
  }

  @Override
  public void noNeedUpdateMembership() {
    this.needUpdateMembershipCount.set(NO_NEED_UPDATE_MEMBERSHIP_FLAG);
  }

  @Override
  public IoActionContext getIoActionContext() {
    return ioActionContext;
  }

  @Override
  public void setIoActionContext(IoActionContext ioActionContext) {
    this.ioActionContext = ioActionContext;
  }

  protected void watchIfCostMoreTime(TraceAction traceAction) {
    long costTime = System.currentTimeMillis() - startIoTime;
    if (costTime > CoordinatorConfigSingleton.getInstance().getDebugIoTimeoutMsThreshold()) {
      logger.info(
          "please watch ori:{}, {} cost time:{}ms, last io action context:{}, failure times:{} "
              + "at:{}",
          requestId, traceAction, costTime, ioActionContext, getFailTimes(), getSegId());
    }
    if (CoordinatorConfigSingleton.getInstance().isTraceAllLogs()) {
      LoggerTracer.getInstance().mark(getRequestId(), "AbstractIoContextManager",
          "please watch ori:{}, {} cost time:{}ms, last io action context:{}, failure times:{} "
              + "at:{}",
          requestId, traceAction, costTime, ioActionContext, getFailTimes(), getSegId());
    }
    LoggerTracer.getInstance().doneTrace(getRequestId(), traceAction,
        CoordinatorConfigSingleton.getInstance().getDebugIoTimeoutMsThreshold());
  }

  @Override
  public long getOriRequestId() {
    return requestId;
  }

  @Override
  public void triggeredByCheckCallback() {
    throw new NotImplementedException("not implement here");
  }

  @Override
  public void markDoneDirectly() {
    this.doneDirectly.set(true);
  }

  @Override
  public void markRequestFailed(InstanceId whoIsDisconnect) {
    SegmentMembership membership = ioActionContext.getMembershipWhenIoCome();
    Validate.notNull(membership);
    MemberIoStatus memberIoStatus = membership.getMemberIoStatus(whoIsDisconnect);

    if (memberIoStatus.isPrimary()) {
      primaryDisconnectCount.compareAndSet(0, 1);
    } else if (memberIoStatus.isSecondary()) {
      secondaryDisconnectCount.incrementAndGet();
    } else if (memberIoStatus.isJoiningSecondary()) {
      joiningSecondaryDisconnectCount.incrementAndGet();
    } else if (memberIoStatus.isArbiter()) {
      arbiterDisconnectCount.incrementAndGet();
    } else {
      Validate
          .isTrue(false, "segment membership:" + membership + ", disconnect:" + whoIsDisconnect);
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
  public void markDelay() {
    needDelay.set(true);
  }

  @Override
  public void resetDelay() {
    needDelay.set(false);
  }

  @Override
  public boolean streamIO() {
    return streamIO;
  }

  @Override
  public void doneForCommitLog() {
    throw new NotImplementedException("not implement here");
  }

  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {
    // do nothing
  }

  public int randomLittleDelayTime() {
    int randomDelayTimeMs = RandomUtils.nextInt(LITTLE_DELAY_THRESHOLD_MS);
    return randomDelayTimeMs;
  }

  @Override
  public void markPrimaryRsp() {
    this.primaryRspTimestamp.set(System.nanoTime());
  }

  @Override
  public void markSecondaryRsp() {
    this.secondaryRspTimestamp.set(System.nanoTime());
  }

  @Override
  public long getResponseInterval() {
    return primaryRspTimestamp.get() - secondaryRspTimestamp.get();
  }

  @Override
  public Long getVolumeId() {
    return volumeId;
  }


  @Override
  public void initRequestCount(int requestCount) {
    this.requestCount.set(requestCount);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractIoContextManager)) {
      return false;
    }

    AbstractIoContextManager that = (AbstractIoContextManager) o;

    if (requestId != that.requestId) {
      return false;
    }
    if (logicalSegIndex != that.logicalSegIndex) {
      return false;
    }
    return volumeId != null ? volumeId.equals(that.volumeId) : that.volumeId == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (requestId ^ (requestId >>> 32));
    result = 31 * result + (volumeId != null ? volumeId.hashCode() : 0);
    result = 31 * result + logicalSegIndex;
    return result;
  }

  protected void cleanUnusedObjects() {
    if (callback != null) {
      callback.releaseReference();
      callback = null;
    }
    requestBuilder = null;
  }

  @Override
  public void releaseReference() {
    this.coordinator = null;
    
  }
}
