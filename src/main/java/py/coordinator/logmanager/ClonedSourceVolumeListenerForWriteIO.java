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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.buffer.PyBuffer;
import py.common.LoggerTracer;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounit.WriteIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.IoUnitContextImpl;
import py.coordinator.iorequest.iounitcontextpacket.SkipForSourceVolumeIoUnitContextPacket;
import py.coordinator.log.BroadcastLog;
import py.coordinator.log.BroadcastLogForLinkedCloneVolume;
import py.coordinator.task.ResendRequest;
import py.icshare.BroadcastLogStatus;


public class ClonedSourceVolumeListenerForWriteIO implements ClonedSourceVolumeReadListener {

  private static final Logger logger = LoggerFactory
      .getLogger(ClonedSourceVolumeListenerForWriteIO.class);
  private static final String className = "ClonedSourceVolumeListenerForWriteIO";
  private final WriteIoContextManager sourceWriteIoContextManager;
  private final Map<IoUnitContext, BroadcastLog> logsToCreateForSkipCloneVolume;
  private final Map<IoUnitContext, BroadcastLog> logsToCreateAfterGotSourceVolume;
  private ReadIoContextManager readIoContextManager;
  private WriteIoContextManager newWriteIoContextManager;
  private ClonedProcessStatus clonedProcessStatus;



  public ClonedSourceVolumeListenerForWriteIO(
      WriteIoContextManager sourceWriteIoContextManager,
      Map<IoUnitContext, BroadcastLog> logsToCreateForSkipCloneVolume) {
    this.sourceWriteIoContextManager = sourceWriteIoContextManager;
    this.logsToCreateForSkipCloneVolume = logsToCreateForSkipCloneVolume;
    this.clonedProcessStatus = ClonedProcessStatus.Step_Begin;
    this.logsToCreateAfterGotSourceVolume = new HashMap<>();
  }

  public void setReadIoContextManager(ReadIoContextManager readIoContextManager) {
    this.readIoContextManager = readIoContextManager;
  }

  @Override
  public void done() {
    logger.debug("got a done result with step {}", clonedProcessStatus);
    if (clonedProcessStatus == ClonedProcessStatus.Step_Begin) {
      processReadDone();
      return;
    } else if (clonedProcessStatus == ClonedProcessStatus.Step_ReadDone) {
      processWriteDone();
      return;
    } else if (clonedProcessStatus == ClonedProcessStatus.Step_WriteDone) {
      if (newWriteIoContextManager != null) {
        newWriteIoContextManager.getCallback().getIoContext().stream().forEach(v -> {
          v.releaseReference(false);
        });
      }
      setHasDone();
    }
  }

  @Override
  public boolean isDone() {
    return clonedProcessStatus == ClonedProcessStatus.Step_Close;
  }

  @Override
  public void failed(Throwable throwable) {
    logger.error("caught an error", throwable);
  }

  private void processReadDone() {
    AtomicInteger count = new AtomicInteger(logsToCreateForSkipCloneVolume.size());
    List<IoUnitContext> ioUnitContexts = readIoContextManager.getCallback().getIoContext();
    List<IoUnitContext> writeIoUnitContexts = new ArrayList<>();
    for (IoUnitContext readUnitContext : ioUnitContexts) {
      BroadcastLog log = logsToCreateForSkipCloneVolume.get(readUnitContext);
      Validate.notNull(log);
      if (log.getIoContext() != null) {
        logger.debug("begin process one log {}, read unit context {}", log, readUnitContext);
        if (readUnitContext.getIoUnit().isSuccess()) {
          IoUnit sourceIoUnit = log.getIoContext().getIoUnit();
          PyBuffer sourceData = sourceIoUnit.getPyBuffer();
          Validate.notNull(readUnitContext);
          IoUnit clonedVolumeIoUnit = readUnitContext.getIoUnit();
          PyBuffer clonedVolumeData = clonedVolumeIoUnit.getPyBuffer();
          int mergedDataOffset = (int) (sourceIoUnit.getOffset() - clonedVolumeIoUnit
              .getOffset());

          Validate.isTrue(sourceData.getByteBuf().readableBytes() == sourceIoUnit.getLength());
          Validate.isTrue(
              clonedVolumeData.getByteBuf().readableBytes() == clonedVolumeIoUnit.getLength());

          sourceData.getByteBuf()
              .getBytes(sourceData.getByteBuf().readerIndex(), clonedVolumeData.getByteBuf(),
                  clonedVolumeData.getByteBuf().readerIndex() + mergedDataOffset,
                  sourceIoUnit.getLength());
          log.release();
          PyBuffer newBuffer = new PyBuffer(clonedVolumeData.getByteBuf());
          readUnitContext.getIoUnit().releaseReference();

          IoUnit ioUnit = new WriteIoUnitImpl(log.getIoContext().getSegIndex(),
              log.getIoContext().getPageIndexInSegment(), readUnitContext.getIoUnit().getOffset(),
              readUnitContext.getIoUnit().getLength(), newBuffer);
          IoUnitContext ioUnitContext = new IoUnitContextImpl(log.getIoContext().getIoRequest(),
              ioUnit);
          BroadcastLogForLinkedCloneVolume newBroadcastLog = new BroadcastLogForLinkedCloneVolume(
              log.getLogUuid(), ioUnitContext, log.getSnapshotVersion(),
              log.getOffset(), log.getLength(), log
          );

          logsToCreateAfterGotSourceVolume.put(ioUnitContext, newBroadcastLog);
          writeIoUnitContexts.add(ioUnitContext);
          logger.debug("add one io context {}, unit {}", log, ioUnit);
        } else {
          logger.warn("ori:{} read from source volume failed {}. source ori:{}",
              readIoContextManager.getRequestId(), readUnitContext.getIoUnit(),
              sourceWriteIoContextManager.getRequestId());
          log.getIoContext().getIoUnit().setSuccess(false);
          log.setStatus(BroadcastLogStatus.Abort);
          log.release();
          log.done();
        }
      } else {
        logger.debug("log io context is null, may it has done", log);
      }
      count.decrementAndGet();
    }

    Validate.isTrue(0 == count.get());
    if (writeIoUnitContexts.size() != 0) {
      SkipForSourceVolumeIoUnitContextPacket ioUnitContextPacket =
          new SkipForSourceVolumeIoUnitContextPacket(
              sourceWriteIoContextManager.getVolumeId(), 0,
              writeIoUnitContexts,
              sourceWriteIoContextManager.getLogicalSegmentIndex(), IoRequestType.Write, this
          );

      newWriteIoContextManager = new WriteIoContextManager(
          sourceWriteIoContextManager.getVolumeId(),
          sourceWriteIoContextManager.getSegId(), ioUnitContextPacket,
          sourceWriteIoContextManager.getCoordinator(),
          new ArrayList<>(logsToCreateAfterGotSourceVolume.values()),
          IoRequestType.Write);
      newWriteIoContextManager.setRequestBuilder(sourceWriteIoContextManager.getRequestBuilder());
      newWriteIoContextManager.setExpiredTime(sourceWriteIoContextManager.getExpiredTime());
      Validate.isTrue(sourceWriteIoContextManager.getRequestBuilder() != null);
      Validate.isTrue(newWriteIoContextManager.getLogsToCreate().size() != 0);

      clonedProcessStatus = ClonedProcessStatus.Step_ReadDone;
      ResendRequest resendRequest = new ResendRequest(newWriteIoContextManager, false, 0);

      logger.info("ori:{} begin rewrite partial unit with basic data. source ori:{}",
          resendRequest.getIoContextManager().getRequestId(),
          sourceWriteIoContextManager.getRequestId());
      LoggerTracer.getInstance()
          .mark(sourceWriteIoContextManager.getRequestId(), className,
              "ori:{} begin rewrite partial unit with basic data. source ori:{}",
              newWriteIoContextManager.getRequestId(),
              sourceWriteIoContextManager.getRequestId());
      sourceWriteIoContextManager.getCoordinator().getDelayManager().put(resendRequest);
    } else {
      clonedProcessStatus = ClonedProcessStatus.Step_WriteDone;
      sourceWriteIoContextManager.doResultForLinkedCloneVolume(this);
    }

  }

  private void processWriteDone() {

    clonedProcessStatus = ClonedProcessStatus.Step_WriteDone;
    sourceWriteIoContextManager.doResultForLinkedCloneVolume(this);
  }

  private void setHasDone() {
    clonedProcessStatus = ClonedProcessStatus.Step_Close;
  }
}
