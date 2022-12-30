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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.buffer.PyBuffer;
import py.common.LoggerTracer;
import py.common.TraceAction;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.SkipForSourceVolumeIoUnitContextPacket;
import py.coordinator.lib.Coordinator;
import py.coordinator.task.ResendRequest;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SecondariesCountInfo;
import py.membership.SegmentForm;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.utils.ReadMergeHelper;
import py.volume.VolumeType;


public class ReadIoContextManager extends AbstractIoContextManager {

  protected static final Logger logger = LoggerFactory.getLogger(ReadIoContextManager.class);
  private Map<IoMember, PyReadResponse> readResponseMap;
  private List<IoUnitContext> requestUnits;
  private List<IoUnitContext> skipForLinkedCloneVolumeUnits;
  private byte[] zeroBuffer;
  private Set<Long> relatedPageIndexes;
  private String className;



  public ReadIoContextManager(Long volumeId, SegId segId, IoUnitContextPacket callback,
      Coordinator coordinator) {
    super(volumeId, segId, callback, CoordinatorConfigSingleton.getInstance().isStreamIo(),
        coordinator);
    this.requestUnits = callback.getIoContext();
    skipForLinkedCloneVolumeUnits = new ArrayList<>();
    this.readResponseMap = new ConcurrentHashMap<>();

    this.className = "ReadIoContextManager";
  }


  public static boolean mergeReadResponses(Map<IoMember, PyReadResponse> readResponseMap,
      IoActionContext ioActionContext, VolumeType volumeType) {
    int goodPrimaryCount = 0;
    int goodJoiningSecondariesCount = 0;
    int arbiterCount = 0;
    SecondariesCountInfo secondariesCountInfo = new SecondariesCountInfo();
    for (Map.Entry<IoMember, PyReadResponse> entry : readResponseMap.entrySet()) {
      if (entry == null || entry.getKey() == null || entry.getValue() == null) {
        Validate.isTrue(false, "can not null" + entry);
      }
      IoMember ioMember = entry.getKey();
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();



      if (memberIoStatus.isPrimary()) {
        goodPrimaryCount++;
      } else if (memberIoStatus.isSecondary()) {
        if (ioActionContext.getFetchReader().contains(ioMember)) {
          secondariesCountInfo.addFetchCount();
        } else if (ioActionContext.getCheckReaders().contains(ioMember)) {
          secondariesCountInfo.addCheckCount();
        } else {
          Validate.isTrue(false, "Secondary can not be other value:" + ioMember.toString());
        }
      } else if (memberIoStatus.isJoiningSecondary()) {
        goodJoiningSecondariesCount++;
      } else if (memberIoStatus.isArbiter()) {
        arbiterCount++;
      } else {
        Validate.isTrue(false, "read response can not be other status");
      }

    }

    SegmentForm segmentForm = ioActionContext.getSegmentForm();
    boolean mergeResult = segmentForm
        .mergeReadLogResult(goodPrimaryCount, secondariesCountInfo, goodJoiningSecondariesCount,
            arbiterCount,
            ioActionContext, volumeType);
    logger.info(
        "get mergeReadResponses, goodPrimaryCount:{}  goodSecondariesCount:{} "
            + "goodJoiningSecondariesCount:{}  arbiterCount:{} mergeResult:{} ",
        goodPrimaryCount, secondariesCountInfo, goodJoiningSecondariesCount, arbiterCount,
        mergeResult);

    return mergeResult;
  }


  public static boolean mergeReadResponses(ReadMethodCallback[] readMethodCallbacks,
      IoActionContext ioActionContext,
      VolumeType volumeType) {
    int goodPrimaryCount = 0;
    int goodJoiningSecondariesCount = 0;
    int arbiterCount = 0;
    SecondariesCountInfo secondariesCountInfo = new SecondariesCountInfo();

    for (int i = 0; i < readMethodCallbacks.length; i++) {
      IoMethodCallback readMethodCallback = readMethodCallbacks[i];

      if (readMethodCallback.weakGoodResponse()) {
        MemberIoStatus memberIoStatus = readMethodCallback.getMemberIoStatus();
        IoMember ioMember = readMethodCallback.getIoMember();
        if (memberIoStatus.isPrimary()) {
          goodPrimaryCount++;
        } else if (memberIoStatus.isSecondary()) {
          if (ioActionContext.getFetchReader().contains(ioMember)) {
            secondariesCountInfo.addFetchCount();
          } else if (ioActionContext.getCheckReaders().contains(ioMember)) {
            secondariesCountInfo.addCheckCount();
          } else {
            Validate.isTrue(false, "Secondary can not be other value:{}", ioMember.toString());
          }
        } else if (memberIoStatus.isJoiningSecondary()) {
          goodJoiningSecondariesCount++;
        } else if (memberIoStatus.isArbiter()) {
          arbiterCount++;
        } else {
          Validate.isTrue(false, "read response can not be other status");
        }
      }
    }

    SegmentForm segmentForm = ioActionContext.getSegmentForm();
    boolean mergeResult = segmentForm
        .mergeReadLogResult(goodPrimaryCount, secondariesCountInfo, goodJoiningSecondariesCount,
            arbiterCount,
            ioActionContext, volumeType);
    logger.info(
        "get mergeReadResponses, goodPrimaryCount:{}  goodSecondariesCount:{} "
            + "goodJoiningSecondariesCount:{}  arbiterCount:{} mergeResult:{} ",
        goodPrimaryCount, secondariesCountInfo, goodJoiningSecondariesCount, arbiterCount,
        mergeResult);

    return mergeResult;
  }

  @Override
  public void doResultForLinkedCloneVolume(ClonedSourceVolumeReadListener listener) {
    reentrantLock.lock();
    try {
      listener.done();

      if (requestUnits.size() == 0 && allClonedSourceVolumeListenerHadDone()) {
        logger.debug("ori:{} read done", getRequestId());
        done(false);
        return;
      } else {
        logger.info("ori:{} read can not get enough good responses, still has:{} logs:{}",
            getRequestId(),
            requestUnits.size(), requestUnits);
        LoggerTracer.getInstance().mark(getRequestId(), className,
            "ori:{} read can not get enough good responses, still has:{} logs:{}",
            getRequestId(),
            requestUnits.size(), requestUnits);
      }


      if (isExpired()) {
        if (!allClonedSourceVolumeListenerHadDone()) {

          return;
        }
        List<IoUnitContext> allUnits = new ArrayList<>();
        allUnits.addAll(requestUnits);
        allUnits.addAll(skipForLinkedCloneVolumeUnits);
        for (IoUnitContext ioContext : allUnits) {
          IoUnit readUnit = ioContext.getIoUnit();
          if (streamIO) {
            readUnit.setSuccess(true);
            // fake read data for stream io
            int size = readUnit.getLength();
            if (zeroBuffer == null) {
              zeroBuffer = new byte[size];
            } else {
              if (zeroBuffer.length < size) {
                zeroBuffer = new byte[size];
              }
            }

            ByteBuf fakeData = Unpooled.wrappedBuffer(zeroBuffer, 0, size);
            PyBuffer pyBuffer = new PyBuffer(fakeData);
            readUnit.setPyBuffer(pyBuffer);
          } else {
            readUnit.setSuccess(false);
            logger.error("requestId:{} read log:{} timeout", getRequestId(), readUnit);
          }
          ioContext.done();
        }
        logger.error("ori:{} read is EXPIRED, response to client", getRequestId());
        LoggerTracer.getInstance()
            .mark(getRequestId(), className, "ori:{} read has expired, response to client",
                getRequestId());
        done(false);
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
      try {
        if (mergeReadResponses(readResponseMap, getIoActionContext(),
            coordinator.getVolumeType(getVolumeId()))) {
          logger.debug("read members has response, read request id:{} at segId:{}",
              getRequestId(), getSegId());
          skipForLinkedCloneVolumeUnits.clear();
          requestUnits = processResponse();
        }
      } finally {
        releaseResponses();
      }


      if (requestUnits.size() == 0 && skipForLinkedCloneVolumeUnits.size() == 0
          && allClonedSourceVolumeListenerHadDone()) {
        logger.debug("ori:{} read done", getRequestId());
        done(false);
        return;
      } else {
        logger.info("ori:{} read can not get enough good responses, still has:{} logs:{}",
            getRequestId(),
            requestUnits.size(), requestUnits);
        LoggerTracer.getInstance().mark(getRequestId(), className,
            "ori:{} read can not get enough good responses, still has:{} logs:{}",
            getRequestId(),
            requestUnits.size(), requestUnits);
      }


      if (isExpired()) {
        if (!allClonedSourceVolumeListenerHadDone()) {

          return;
        }
        List<IoUnitContext> allUnits = new ArrayList<>();
        allUnits.addAll(requestUnits);
        allUnits.addAll(skipForLinkedCloneVolumeUnits);
        for (IoUnitContext ioContext : allUnits) {
          IoUnit readUnit = ioContext.getIoUnit();
          if (streamIO) {
            readUnit.setSuccess(true);

            int size = readUnit.getLength();
            if (zeroBuffer == null) {
              zeroBuffer = new byte[size];
            } else {
              if (zeroBuffer.length < size) {
                zeroBuffer = new byte[size];
              }
            }

            ByteBuf fakeData = Unpooled.wrappedBuffer(zeroBuffer, 0, size);
            PyBuffer pyBuffer = new PyBuffer(fakeData);
            readUnit.setPyBuffer(pyBuffer);
          } else {
            readUnit.setSuccess(false);
            logger.error("requestId:{} read log:{} timeout", getRequestId(), readUnit);
          }
          ioContext.done();
        }
        logger.error("ori:{} read is EXPIRED, response to client", getRequestId());
        LoggerTracer.getInstance()
            .mark(getRequestId(), className, "ori:{} read has expired, response to client",
                getRequestId());
        done(false);
        return;
      }

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
              "ori:{} at:{} re-read, primary dis count:{}, secondary dis count:{}, joining "
                  + "secondary dis count:{}, arbiter dis count:{}, segment form:{}, done "
                  + "directly:{}",
              getRequestId(), segId, primaryDisconnectCount.get(),
              secondaryDisconnectCount.get(),
              joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(),
              segmentForm, doneDirectly);
          LoggerTracer.getInstance().mark(getRequestId(), className,
              "ori:{} at:{} re-read, primary dis count:{}, secondary dis count:{}, joining "
                  + "secondary dis count:{}, arbiter dis count:{}, segment form:{}, done "
                  + "directly:{}",
              getRequestId(), segId, primaryDisconnectCount.get(),
              secondaryDisconnectCount.get(),
              joiningSecondaryDisconnectCount.get(), arbiterDisconnectCount.get(),
              segmentForm, doneDirectly);
        }
        resetRequestFailedInfo();
      }



      if (requestUnits.size() != 0) {
        IoActionContext ioActionContext = getIoActionContext();
        Set<IoMember> ioMembers = ioActionContext.getIoMembers();
        boolean needUpdateMembership = needUpdateMembership(
            coordinator.getVolumeType(getVolumeId()).getWriteQuorumSize(),
            ioMembers.size());
        ResendRequest resendRequest = new ResendRequest(this, needUpdateMembership,
            needDelay.get() ? DEFAULT_DELAY_MS : randomLittleDelayTime());

        resetNeedUpdateMembership();
        resetDelay();
        coordinator.getDelayManager().put(resendRequest);
      }



      if (skipForLinkedCloneVolumeUnits.size() != 0) {

        // this request.
        Long sourceVolumeId = coordinator.getVolumeMetaData(volumeId).getSourceVolumeId();
        Integer sourceSnapshotId = coordinator.getVolumeMetaData(volumeId)
            .getSourceSnapshotId();


        ClonedSourceVolumeReadListener listener = new ClonedSourceVolumeListenerForReadIO(
            this);


        SkipForSourceVolumeIoUnitContextPacket callback =
            new SkipForSourceVolumeIoUnitContextPacket(
                sourceVolumeId, sourceSnapshotId, skipForLinkedCloneVolumeUnits,
                skipForLinkedCloneVolumeUnits.get(0).getSegIndex(),
                skipForLinkedCloneVolumeUnits.get(0).getRequestType(), listener);
        addClonedSourceVolumeListener(listener);


        SegId sourceSegId = new SegId(sourceVolumeId, segId.getIndex());
        ReadIoContextManager ioContextManager = new ReadIoContextManager(sourceVolumeId,
            sourceSegId, callback, coordinator);
        Set<Long> pageIndexes = new HashSet<>();
        for (IoUnitContext ioContext : callback.getIoContext()) {
          if (!ioContext.hasDone()) {
            pageIndexes.add(ioContext.getPageIndexInSegment());
          }
        }
        ioContextManager.setRelatedPageIndexes(pageIndexes);
        ioContextManager.setExpiredTime(this.getExpiredTime());


        final ResendRequest resendRequest = new ResendRequest(ioContextManager, false,
            0);

        resetNeedUpdateMembership();
        resetDelay();

        LoggerTracer.getInstance()
            .mark(this.getRequestId(), className,
                "ori:{} begin read data from source data for linked clone. source ori:{}",
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
    logger.info("ori:{} read done directly at logic segment:{}", getRequestId(),
        getLogicalSegmentIndex());
    List<IoUnitContext> allUnits = new ArrayList<>();
    allUnits.addAll(requestUnits);
    allUnits.addAll(skipForLinkedCloneVolumeUnits);
    // prepare all request unit
    for (IoUnitContext ioContext : allUnits) {
      IoUnit readUnit = ioContext.getIoUnit();
      readUnit.setSuccess(true);

      int size = readUnit.getLength();
      if (zeroBuffer == null) {
        zeroBuffer = new byte[size];
      } else {
        if (zeroBuffer.length < size) {
          zeroBuffer = new byte[size];
        }
      }

      ByteBuf fakeData = Unpooled.wrappedBuffer(zeroBuffer, 0, size);
      PyBuffer pyBuffer = new PyBuffer(fakeData);
      readUnit.setPyBuffer(pyBuffer);
      ioContext.done();
    }

    done(true);
  }

  private void done(boolean doneDirectly) {
    watchIfCostMoreTime(TraceAction.Read);
    IoUnitContextPacket callback = getCallback();
    if (callback != null) {
      callback.complete();
    }
    cleanUnusedObjects();
  }

  private void releaseResponses() {
    for (Map.Entry<IoMember, PyReadResponse> entry : readResponseMap.entrySet()) {
      if (entry == null || entry.getValue() == null) {
        Validate.isTrue(false, "can not null: " + entry);
      }
      PyReadResponse readResponse = entry.getValue();
      Validate.notNull(readResponse);
      readResponse.release();
    }
  }


  private PyReadResponse[] getRealReadResponses() {
    List<PyReadResponse> realReadResponseList = new ArrayList<>();
    if (!getIoActionContext().getFetchReader().isEmpty()) {
      for (Map.Entry<IoMember, PyReadResponse> entry : readResponseMap.entrySet()) {
        if (entry == null || entry.getKey() == null || entry.getValue() == null) {
          Validate.isTrue(false, "can not null" + entry);
        }
        IoMember ioMember = entry.getKey();
        PyReadResponse readResponse = entry.getValue();
        if (ioMember.isFetchRead()) {
          if (readResponse == null) {
            Validate.isTrue(false,
                "ori:" + getRequestId() + "PyReadResponse can not be null, at:" + segId + ", "
                    + ioMember);
          }
          Broadcastlog.PbReadResponse pbReadResponse = readResponse.getMetadata();
          if (pbReadResponse == null) {
            Validate.isTrue(false,
                "ori:" + getRequestId() + "PbReadResponse can not be null, at:" + segId + ", "
                    + ioMember);
          } else {
            realReadResponseList.add(readResponse);
          }
        }
      }
    } else {
      Validate.isTrue(false, "currently, we do not support merge read ! " + readResponseMap);
    }

    if (realReadResponseList.size() != 1) {
      Validate.isTrue(false, "fetch read count is not equal to one");
    }

    PyReadResponse[] responses = new PyReadResponse[realReadResponseList.size()];
    for (int index = 0; index < responses.length; index++) {
      responses[index] = realReadResponseList.get(index);
    }
    Validate.notEmpty(responses);
    return responses;
  }

  private List<IoUnitContext> processResponse() {
    PyReadResponse[] realReadResponses = getRealReadResponses();

    if (realReadResponses == null) {
      return requestUnits;
    }

    PyReadResponse response = ReadMergeHelper
        .merge(realReadResponses, CoordinatorConfigSingleton.getInstance().getPageSize());
    List<IoUnitContext> contextSkipped = new ArrayList<>();
    int i = 0;

    for (PbReadResponseUnit responseUnit : response.getMetadata().getResponseUnitsList()) {
      int index = i++;
      IoUnitContext ioContext = requestUnits.get(index);
      if (ioContext.hasDone()) {
        continue;
      }

      IoUnit originalUnit = ioContext.getIoUnit();
      Validate.isTrue(responseUnit.getOffset() == originalUnit.getOffset());
      Validate.isTrue(responseUnit.getLength() == originalUnit.getLength());
      PbIoUnitResult result = responseUnit.getResult();
      if (result == PbIoUnitResult.OK || result == PbIoUnitResult.MERGE_OK) {
        ByteBuf readData = response.getResponseUnitData(index);
        originalUnit.setSuccess(true);
        originalUnit.setPyBuffer(new PyBuffer(readData));

        logger.debug("requestId:{} read unit:{} unit PyBuffer:{} success", getRequestId(),
            originalUnit, originalUnit.getPyBuffer());
        ioContext.done();
      } else if (result == PbIoUnitResult.FREE) {
        if (coordinator.isLinkedCloneVolume(volumeId)) {
          skipForLinkedCloneVolumeUnits.add(ioContext);
        } else {
          originalUnit.setSuccess(true);
          int size = originalUnit.getLength();
          if (zeroBuffer == null) {
            zeroBuffer = new byte[size];
          } else {
            if (zeroBuffer.length < size) {
              zeroBuffer = new byte[size];
            }
          }

          originalUnit
              .setPyBuffer(new PyBuffer(Unpooled.wrappedBuffer(zeroBuffer, 0, size)));
          logger.debug("requestId={} read unit:{} success, and empty", getRequestId(),
              originalUnit);
          ioContext.done();
        }
      } else if (result == PbIoUnitResult.INPUT_HAS_NO_DATA
          || result == PbIoUnitResult.OUT_OF_RANGE) {
        originalUnit.setSuccess(false);
        logger
            .error("ori:{} read unit:{} failure, result: {}", getRequestId(), originalUnit, result);
        ioContext.done();
      } else if (result == PbIoUnitResult.SKIP) {
        logger.warn("ori:{} datanode skip the unit: {}, just retry", getRequestId(), originalUnit);
        contextSkipped.add(ioContext);
      } else {
        logger.warn("ori:{} let's have a look at:{}", result);
        contextSkipped.add(ioContext);
      }
    }
    return contextSkipped;
  }

  @Override
  public void processResponse(Object object, IoMember ioMember) {
    Validate.notNull(ioMember);
    if (object != null) {
      this.readResponseMap.put(ioMember, (PyReadResponse) object);
    }

    int requestCount = super.requestCount.decrementAndGet();
    if (requestCount != 0) {
      return;
    }
    doResult();
  }

  @Override
  public void initRequestCount(int requestCount) {
    this.readResponseMap.clear();
    super.initRequestCount(requestCount);
  }

  @Override
  public void releaseReference() {
    super.releaseReference();
  }

  @Override
  protected void cleanUnusedObjects() {
    super.cleanUnusedObjects();

    readResponseMap.clear();
    readResponseMap = null;

    if (requestUnits != null) {
      requestUnits.clear();
      requestUnits = null;
    }
    releaseReference();
  }

  public List<IoUnitContext> getRequestUnits() {
    return requestUnits;
  }

  public Set<Long> getRelatedPageIndexes() {
    return relatedPageIndexes;
  }

  public void setRelatedPageIndexes(Set<Long> relatedPageIndexes) {
    this.relatedPageIndexes = relatedPageIndexes;
  }
}
