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

package py.coordinator.lib;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.buffer.PyBuffer;
import py.common.struct.Pair;
import py.coordinator.calculator.LogicalToPhysicalCalculator;
import py.coordinator.calculator.StripeLogicalToPhysicalCalculator;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestImpl;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounit.ReadIoUnitImpl;
import py.coordinator.iorequest.iounit.WriteIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.IoUnitContextImpl;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public class IoConvertorImpl implements IoConvertor {

  private static final Logger logger = LoggerFactory.getLogger(IoRequestImpl.class);
  private final IoSeparatorImpl ioSeparator;
  private final StorageDriver storageDriver;
  private final long segmentSize;
  private final long pageSize;
  private final Map<Long, Map<Integer, List<IoUnitContext>>> mapRequestUuidToWriteContexts;
  private final Map<Long, Map<Integer, List<IoUnitContext>>> mapRequestUuidToReadContexts;
  private final Map<Long, Map<Integer, List<IoUnitContext>>> mapRequestUuidToDiscardContexts;
  private final VolumeInfoHolderManager volumeInfoHolderManager;
  private final long checkValue = -1L;
  private final long valueReset = 0L;



  public IoConvertorImpl(StorageDriver storageDriver,
      VolumeInfoHolderManager volumeInfoHolderManager,
      IoSeparatorImpl ioSeparator, long segmentSize, int pageSize) {
    this.storageDriver = storageDriver;
    this.volumeInfoHolderManager = volumeInfoHolderManager;
    this.segmentSize = segmentSize;
    this.pageSize = pageSize;
    this.ioSeparator = ioSeparator;
    this.mapRequestUuidToWriteContexts = new ConcurrentHashMap<>();
    this.mapRequestUuidToReadContexts = new ConcurrentHashMap<>();
    this.mapRequestUuidToDiscardContexts = new ConcurrentHashMap<>();
  }


  public IoSeparatorImpl getIoSeparator() {
    return ioSeparator;
  }

  @Override
  public void processWriteRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException {
    ByteBuf body;
    try {

      body = ioRequest.getBody();
      Validate.notNull(body);

      long position = ioRequest.getOffset();

      long processingLen;
      if (logger.isDebugEnabled()) {
        logger.debug("process write request:{}", ioRequest);
      }
      Long volumeId = ioRequest.getVolumeId();
      LogicalToPhysicalCalculator calculator = volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getLogicalToPhysicalCalculator();
      Validate.notNull(calculator);

      int lastPhysicalSegIndex = -1;
      int touchSegmentCount = 0;

      while (body.isReadable()) {
        int logicalSegIndex = calculator.calculateIndex(position);
        long offsetInSegmentUnit = position - logicalSegIndex * segmentSize;
        processingLen = segmentSize - offsetInSegmentUnit;
        processingLen = Math.min(processingLen, body.readableBytes());

        long leftSize = processingLen;
        long unitSize;
        while (leftSize > 0) {
          unitSize = pageSize - (int) (offsetInSegmentUnit % pageSize);
          unitSize = Math.min(unitSize, leftSize);

          Validate.isTrue(unitSize > 0);
          Pair<Integer, Long> physicalValue = calculator.convertLogicalPositionToPhysical(position,
              StripeLogicalToPhysicalCalculator.MAX_GET_VOLUME_TIMEOUT_MS);
          long pageIndexInSegment = physicalValue.getSecond() / pageSize;

          int physicalSegIndex = physicalValue.getFirst();

          if (lastPhysicalSegIndex < 0) {
            lastPhysicalSegIndex = physicalSegIndex;
            touchSegmentCount++;
          } else {
            if (lastPhysicalSegIndex != physicalSegIndex) {
              lastPhysicalSegIndex = physicalSegIndex;
              touchSegmentCount++;
            }
          }

          ByteBuf data = body.readSlice((int) unitSize).retain();


          IoUnit unit = new WriteIoUnitImpl(physicalSegIndex, pageIndexInSegment,
              physicalValue.getSecond(),
              (int) unitSize, new PyBuffer(data));

          logger.debug(
              "processing write request data. Segment Index: {}, pageIndex: {}, Pos: {}, "
                  + "unitSize: {}, length: {} to write",
              physicalSegIndex, pageIndexInSegment, position, unitSize, processingLen);

          ioRequest.incReferenceCount();
          ioRequest.add(unit);

          if (!mapRequestUuidToWriteContexts.containsKey(requestUuid)) {
            Map<Integer, List<IoUnitContext>> mapIndexToWriteContexts = new ConcurrentHashMap<>();
            mapRequestUuidToWriteContexts.putIfAbsent(requestUuid, mapIndexToWriteContexts);
          }

          Map<Integer, List<IoUnitContext>> mapIndexToWriteContexts = mapRequestUuidToWriteContexts
              .get(requestUuid);

          if (!mapIndexToWriteContexts.containsKey(physicalSegIndex)) {
            List<IoUnitContext> ioContextsInOneSegment = new ArrayList<>();
            mapIndexToWriteContexts.putIfAbsent(physicalSegIndex, ioContextsInOneSegment);
          }

          List<IoUnitContext> ioContextsInOneSegment = mapIndexToWriteContexts
              .get(physicalSegIndex);
          ioContextsInOneSegment.add(new IoUnitContextImpl(ioRequest, unit));

          offsetInSegmentUnit += unitSize;
          position += unitSize;
          leftSize -= unitSize;
        }
      }

      if (fireNow) {
        fireWrite(requestUuid);
      }
    } catch (Throwable t) {
      logger.error("process write request:{} caught an exception", ioRequest.getOffset(), t);
    } finally {
      ioRequest.releaseBody();
    }
  }

  @Override
  public void processReadRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException {
    try {
      long position = ioRequest.getOffset();

      long remainingLen = ioRequest.getLength();
      logger.debug("process read request: {}", ioRequest);

      long processingLen;
      Long volumeId = ioRequest.getVolumeId();
      LogicalToPhysicalCalculator calculator = volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getLogicalToPhysicalCalculator();
      Validate.notNull(calculator);

      int lastPhysicalSegIndex = -1;
      int touchSegmentCount = 0;

      while (remainingLen > 0) {

        int logicalSegIndex = calculator.calculateIndex(position);
        long offsetInSegmentUnit = position - logicalSegIndex * segmentSize;
        processingLen = segmentSize - offsetInSegmentUnit;
        processingLen = Math.min(processingLen, remainingLen);


        long leftSize = processingLen;
        long unitSize;
        while (leftSize > 0) {
          unitSize = pageSize - (int) (offsetInSegmentUnit % pageSize);
          unitSize = Math.min(unitSize, leftSize);

          Pair<Integer, Long> physicalValue = calculator.convertLogicalPositionToPhysical(position,
              StripeLogicalToPhysicalCalculator.MAX_GET_VOLUME_TIMEOUT_MS);
          int physicalSegIndex = physicalValue.getFirst();

          if (lastPhysicalSegIndex < 0) {
            lastPhysicalSegIndex = physicalSegIndex;
            touchSegmentCount++;
          } else {
            if (lastPhysicalSegIndex != physicalSegIndex) {
              lastPhysicalSegIndex = physicalSegIndex;
              touchSegmentCount++;
            }
          }

          long pageIndexInSegment = physicalValue.getSecond() / pageSize;
          ReadIoUnitImpl unit = new ReadIoUnitImpl(physicalValue.getFirst(), pageIndexInSegment,
              physicalValue.getSecond(), (int) unitSize);
          logger.debug(
              "processing {} request data. Segment Index: {}, pageIndex: {}, Pos: {}, unitSize: "
                  + "{}, length: {} to read",
              ioRequest.getIoRequestType(), physicalSegIndex, pageIndexInSegment, position,
              unitSize, processingLen);

          ioRequest.add(unit);
          ioRequest.incReferenceCount();

          if (!mapRequestUuidToReadContexts.containsKey(requestUuid)) {
            Map<Integer, List<IoUnitContext>> mapIndexToReadContexts = new ConcurrentHashMap<>();
            mapRequestUuidToReadContexts.putIfAbsent(requestUuid, mapIndexToReadContexts);
          }

          Map<Integer, List<IoUnitContext>> mapIndexToReadContexts = mapRequestUuidToReadContexts
              .get(requestUuid);

          if (!mapIndexToReadContexts.containsKey(physicalSegIndex)) {
            List<IoUnitContext> ioContextsInOneSegment = new ArrayList<>();
            mapIndexToReadContexts.putIfAbsent(physicalSegIndex, ioContextsInOneSegment);
          }

          List<IoUnitContext> ioContextsInOneSegment = mapIndexToReadContexts.get(physicalSegIndex);
          ioContextsInOneSegment.add(new IoUnitContextImpl(ioRequest, unit));

          position += unitSize;
          leftSize -= unitSize;
          offsetInSegmentUnit += unitSize;
        }

        remainingLen -= processingLen;
      }

      if (fireNow) {
        if (ioRequest.getIoRequestType().isRead()) {
          fireRead(requestUuid);
        } else if (ioRequest.getIoRequestType().isDiscard()) {
          fireDiscard(requestUuid);
        }
      }
    } catch (Throwable t) {
      logger.error("process read request:{} caught an exception", ioRequest.getOffset(), t);
    }
  }

  @Override
  public void processDiscardRequest(Long requestUuid, IoRequest ioRequest, boolean fireNow)
      throws ProcessPositionException, StorageException {
    Map<Long, Map<Integer, List<DiscardInfo>>> mapForDiscard = new ConcurrentHashMap<>();
    try {
      long position = ioRequest.getOffset();

      long remainingLen = ioRequest.getLength();
      logger.debug("process read request: {}", ioRequest);

      long processingLen;
      Long volumeId = ioRequest.getVolumeId();
      LogicalToPhysicalCalculator calculator = volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getLogicalToPhysicalCalculator();
      Validate.notNull(calculator);
      while (remainingLen > 0) {

        int logicalSegIndex = calculator.calculateIndex(position);
        long offsetInSegmentUnit = position - logicalSegIndex * segmentSize;
        processingLen = segmentSize - offsetInSegmentUnit;
        processingLen = Math.min(processingLen, remainingLen);


        long leftSize = processingLen;
        long unitSize;
        while (leftSize > 0) {
          unitSize = pageSize - (int) (offsetInSegmentUnit % pageSize);
          unitSize = Math.min(unitSize, leftSize);

          Pair<Integer, Long> physicalValue = calculator.convertLogicalPositionToPhysical(position,
              StripeLogicalToPhysicalCalculator.MAX_GET_VOLUME_TIMEOUT_MS);
          int physicalSegIndex = physicalValue.getFirst();
          long pageIndexInSegment = physicalValue.getSecond() / pageSize;
          long eachOffset = physicalValue.getSecond();

          final DiscardInfo discardInfo = new DiscardInfo(eachOffset, (int) unitSize);
          logger.debug(
              "processing {} request data. Segment Index: {}, pageIndex: {}, offset : {}, Pos: "
                  + "{}, unitSize: {}, length: {} to read",
              ioRequest.getIoRequestType(), physicalSegIndex, pageIndexInSegment,
              physicalValue.getSecond(), position, unitSize, processingLen);

          if (!mapForDiscard.containsKey(requestUuid)) {
            Map<Integer, List<DiscardInfo>> mapIndexToReadContexts = new ConcurrentHashMap<>();
            mapForDiscard.putIfAbsent(requestUuid, mapIndexToReadContexts);
          }

          Map<Integer, List<DiscardInfo>> mapIndexToReadContexts = mapForDiscard.get(requestUuid);

          if (!mapIndexToReadContexts.containsKey(physicalSegIndex)) {
            List<DiscardInfo> ioContextsInOneSegment = new ArrayList<>();
            mapIndexToReadContexts.putIfAbsent(physicalSegIndex, ioContextsInOneSegment);
          }

          List<DiscardInfo> ioContextsInOneSegment = mapIndexToReadContexts.get(physicalSegIndex);
          ioContextsInOneSegment.add(discardInfo);

          position += unitSize;
          leftSize -= unitSize;
          offsetInSegmentUnit += unitSize;
        }

        remainingLen -= processingLen;
      }

      checkDiscardInfo(requestUuid, ioRequest, mapForDiscard);
      if (fireNow) {
        fireDiscard(requestUuid);
      }
    } catch (Throwable t) {
      logger.error("process read request:{} caught an exception", ioRequest.getOffset(), t);
    }
  }


  public void checkDiscardInfo(long requestUuid, IoRequest ioRequest,
      Map<Long, Map<Integer, List<DiscardInfo>>> mapForDiscard) {
    Map<Integer, List<DiscardInfo>> mapIndexToDiscardContexts = mapForDiscard.get(requestUuid);
    if (mapIndexToDiscardContexts != null && !mapIndexToDiscardContexts.isEmpty()) {

      for (Map.Entry<Integer, List<DiscardInfo>> entry : mapIndexToDiscardContexts.entrySet()) {
        List<DiscardInfo> discardInfos = entry.getValue();
        long firstOffset = checkValue;
        long endLength = valueReset;
        for (int i = 0; i < discardInfos.size(); i++) {

          long offset = discardInfos.get(i).offset;
          long length = discardInfos.get(i).length;
          long endOffset = offset + length;


          if (discardInfos.size() - i > 1) {

            if (endOffset == discardInfos.get(i + 1).offset) {

              if (firstOffset == checkValue) {
                firstOffset = offset;
              }
              endLength += length;
              logger.info("begin the firstOffset is: {}, the endLength is: {}", firstOffset,
                  endLength);
            } else {
              // begin different, the first one is different
              if (endLength == valueReset) {
                makeInfoToMap(requestUuid, offset, (int) length, ioRequest, entry.getKey());
              } else {

                makeInfoToMap(requestUuid, firstOffset, (int) (endLength + length), ioRequest,
                    entry.getKey());
                endLength = valueReset;
                firstOffset = checkValue;
              }
            }
          } else {
            if (endLength == 0L) {
              makeInfoToMap(requestUuid, offset, (int) length, ioRequest, entry.getKey());
            } else {

              makeInfoToMap(requestUuid, firstOffset, (int) (endLength + length), ioRequest,
                  entry.getKey());
              endLength = valueReset;
              firstOffset = checkValue;
            }
          }
        }

      }
    }
  }

  private void makeInfoToMap(long requestUuid, long offset, int length, IoRequest ioRequest,
      int physicalSegIndex) {

    long pageIndexInSegment = 0;
    ReadIoUnitImpl unit = new ReadIoUnitImpl(physicalSegIndex, pageIndexInSegment, offset, length);
    ioRequest.add(unit);
    ioRequest.incReferenceCount();

    if (!mapRequestUuidToDiscardContexts.containsKey(requestUuid)) {
      Map<Integer, List<IoUnitContext>> mapIndexToReadContexts = new ConcurrentHashMap<>();
      mapRequestUuidToDiscardContexts.putIfAbsent(requestUuid, mapIndexToReadContexts);
    }

    Map<Integer, List<IoUnitContext>> mapIndexToReadContexts = mapRequestUuidToDiscardContexts
        .get(requestUuid);

    if (!mapIndexToReadContexts.containsKey(physicalSegIndex)) {
      List<IoUnitContext> ioContextsInOneSegment = new ArrayList<>();
      mapIndexToReadContexts.putIfAbsent(physicalSegIndex, ioContextsInOneSegment);
    }

    List<IoUnitContext> ioContextsInOneSegment = mapIndexToReadContexts.get(physicalSegIndex);
    ioContextsInOneSegment.add(new IoUnitContextImpl(ioRequest, unit));
  }



  public void fireWrite(Long requestUuid) throws StorageException {
    try {
      Map<Integer, List<IoUnitContext>> mapIndexToWriteContexts = mapRequestUuidToWriteContexts
          .remove(requestUuid);
      if (mapIndexToWriteContexts != null && !mapIndexToWriteContexts.isEmpty()) {
        for (Map.Entry<Integer, List<IoUnitContext>> entry : mapIndexToWriteContexts.entrySet()) {
          logger.debug("write done with segment index {}, entry: {}", entry.getKey(),
              entry.getValue());
          ioSeparator.splitWrite(entry.getValue());
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new StorageException();
    }
  }



  public void fireRead(Long requestUuid) throws StorageException {
    try {
      Map<Integer, List<IoUnitContext>> mapIndexToReadContexts = mapRequestUuidToReadContexts
          .remove(requestUuid);
      if (mapIndexToReadContexts != null && !mapIndexToReadContexts.isEmpty()) {
        for (Map.Entry<Integer, List<IoUnitContext>> entry : mapIndexToReadContexts.entrySet()) {
          logger.debug("read done with segment index {}, entry: {}", entry.getKey(),
              entry.getValue());
          ioSeparator.splitRead(entry.getValue());
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new StorageException();
    }
  }

  @Override
  public void fireDiscard(Long requestUuid) throws StorageException {
    try {
      Map<Integer, List<IoUnitContext>> mapIndexToDiscardContexts = mapRequestUuidToDiscardContexts
          .remove(requestUuid);
      if (mapIndexToDiscardContexts != null && !mapIndexToDiscardContexts.isEmpty()) {
        for (Map.Entry<Integer, List<IoUnitContext>> entry : mapIndexToDiscardContexts.entrySet()) {
          logger.debug("discard done with segment index {}, entry: {}", entry.getKey(),
              entry.getValue());
          ioSeparator.processDiscard(entry.getValue());
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new StorageException();
    }
  }
}
