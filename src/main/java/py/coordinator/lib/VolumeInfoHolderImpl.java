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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.struct.LimitQueue;
import py.coordinator.calculator.LogicalToPhysicalCalculator;
import py.coordinator.calculator.LogicalToPhysicalCalculatorFactory;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.drivercontainer.exception.InitializationException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.performance.PerformanceManager;
import py.performance.PerformanceRecorder;
import py.thrift.share.StoragePoolThrift;


public class VolumeInfoHolderImpl implements VolumeInfoHolder {

  protected static final Logger logger = LoggerFactory.getLogger(VolumeInfoHolderImpl.class);

  private final Long volumeId;
  private final VolumeInfoRetriever volumeInfoRetriever;
  private final Multimap<Integer, IoUnitContextPacket> ioContextWaitingForVolumeCompletion;
  private final Map<SegId, Integer> mapSegIdToIndex;
  private final Map<SegId, AtomicLong> recordLastIoTime;
  private final AtomicInteger notifyDatanodeCount;
  private final LinkedBlockingDeque<InstanceId> failedDatanode;
  private final Map<Long, Long> mapVolumeIdToRootVolumeId;
  public LogicalToPhysicalCalculator logicalToPhysicalCalculator;
  protected volatile SpaceSavingVolumeMetadata volumeMetadata;
  private StoragePoolThrift storagePoolThrift;

  public VolumeInfoHolderImpl(Long volumeId, int snapshotId,
      VolumeInfoRetriever volumeInfoRetriever,
      Map<Long, Long> mapVolumeIdToRootVolumeId) {
    this.volumeId = volumeId;
    this.volumeInfoRetriever = volumeInfoRetriever;
    this.ioContextWaitingForVolumeCompletion = LinkedListMultimap.create();
    this.mapSegIdToIndex = new ConcurrentHashMap<>();
    this.recordLastIoTime = new ConcurrentHashMap<>();
    this.notifyDatanodeCount = new AtomicInteger(0);
    this.failedDatanode = new LinkedBlockingDeque<>();
    this.mapVolumeIdToRootVolumeId = mapVolumeIdToRootVolumeId;
  }

  @Override
  public void init() throws Exception {
    try {
      updateVolumeMetadata();
      long storagePoolId = volumeMetadata.getStoragePoolId();
      try {
        storagePoolThrift = volumeInfoRetriever
            .getStoragePoolInfo(volumeMetadata.getDomainId(), storagePoolId);
      } catch (Exception e) {
        logger.error("get storage poll error", e);
      }

      if (storagePoolThrift == null) {
        storagePoolThrift = new StoragePoolThrift();
        storagePoolThrift.setPoolId(storagePoolId);
        storagePoolThrift.setPoolName("UNKNOWN");
        logger.warn(
            "when init the VolumeInfoHolderImpl, can not find the pool name about volume :{} and "
                + "Pool id: {}",
            volumeId, storagePoolId);
      }

      LogicalToPhysicalCalculatorFactory factory = new LogicalToPhysicalCalculatorFactory();
      factory.setVolumeInfoRetriever(this.volumeInfoRetriever);
      logicalToPhysicalCalculator = factory.build(this.volumeId);
      logicalToPhysicalCalculator.updateVolumeInformation();

      PerformanceRecorder performanceRecorder = new PerformanceRecorder();
      PerformanceManager.getInstance().addPerformance(volumeId, performanceRecorder);
      logger.warn("init the Performance, the volume id is:{}", volumeId);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
  }

  /**
   * init method executed when bean factory initialize to get volume meta data from infocenter.
   */
  private void updateVolumeMetadata() throws Exception {
    int maxAttempts = 200;
    int retry = 0;
    while (true) {
      try {
        SpaceSavingVolumeMetadata volumeMetadata = volumeInfoRetriever.getVolume(this.volumeId);
        if (volumeMetadata == null) {
          throw new InitializationException("can't retrieve volume metadata:" + this.volumeId);
        } else {
          if (!volumeIsAvailable(volumeMetadata)) {
            throw new InitializationException();
          }
          this.volumeMetadata = volumeMetadata;
          this.volumeMetadata
              .setSegmentSize(CoordinatorConfigSingleton.getInstance().getSegmentSize());
          break;
        }
      } catch (Exception e) {
        logger.warn("can't retrieve volume information, retry time:{}", retry + 1, e);
        if (retry == maxAttempts) {
          throw e;
        } else {
          logger.warn("sleep 3 seconds");
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger.warn("init coordinator, can't sleep", e1);
          }
          retry++;
          continue;
        }
      }
    }

    updateSegIdToIndexMap();
    logger.warn("volume is {}", this.volumeMetadata);
  }

  private void updateVolumeMetadata(SpaceSavingVolumeMetadata volumeMetadata, int startSegmentIndex,
      int updateCount)
      throws Exception {
    Map<Integer, SegmentMembership> mapUpdatingSegIndexToMembership = new HashMap<Integer,
        SegmentMembership>();
    int segmentNumToUpdate = 0;
    for (int i = 0; i < updateCount; i++) {
      int innerIndex = startSegmentIndex + i;
      if (innerIndex > volumeMetadata.getSegmentCount()) {
        break;
      } else if (this.volumeMetadata.getSegId(innerIndex) == null) {
        segmentNumToUpdate++;
      } else {
        break;
      }
    }

    for (int segIndex = startSegmentIndex; segIndex < startSegmentIndex + segmentNumToUpdate;
        segIndex++) {

      SegmentMembership segMetadataToBeAdded = volumeMetadata.getHighestMembership(segIndex);
      SegmentMembership currentMembership = this.volumeMetadata.getHighestMembership(segIndex);
      if (currentMembership == null || currentMembership.compareVersion(segMetadataToBeAdded) < 0) {
        this.volumeMetadata.addMembership(segIndex, segMetadataToBeAdded);
        logger.warn("add new segment {} {}, membership {}", segIndex, segMetadataToBeAdded,
            segMetadataToBeAdded);
        mapUpdatingSegIndexToMembership.put(segIndex, segMetadataToBeAdded);
      }
    }
    this.volumeMetadata.setVolumeSize(volumeMetadata.getVolumeSize());
    logicalToPhysicalCalculator.updateVolumeInformation();
    updateSegIdToIndexMap();
  }

  private void updateSegIdToIndexMap() {
    Map<Integer, LimitQueue<SegmentMembership>> mapIndexToMembership = volumeMetadata
        .getMemberships();
    logger.warn("going to update segId to logic segment index map, current keys count:{}",
        mapIndexToMembership.size());
    for (Integer segIndex : mapIndexToMembership.keySet()) {
      SegId segId = volumeMetadata.getSegId(segIndex);
      mapSegIdToIndex.put(segId, segIndex);
      mapVolumeIdToRootVolumeId
          .putIfAbsent(segId.getVolumeId().getId(), volumeMetadata.getRootVolumeId());
    }
    logger.warn("update segId to logic segment index map:{} complete", mapSegIdToIndex);
    logger.warn("update volumeId to root volumeId map:{} complete", mapVolumeIdToRootVolumeId);
  }

  @Override
  public void updateVolumeMetadataWhenSegmentCreated(int startSegmentIndex) {

    int maxAttempts = 10;
    int retry = 0;
    while (true) {
      try {
        SpaceSavingVolumeMetadata volumeMetadata = volumeInfoRetriever.getVolume(this.volumeId);
        if (volumeMetadata == null) {
          throw new InitializationException("Can't retrieve volume metadata to update");
        } else {
          logger.warn("going to update volume metadata due to new segment created {}",
              startSegmentIndex);
          updateVolumeMetadata(volumeMetadata, startSegmentIndex,
              volumeMetadata.getSegmentNumToCreateEachTime());
          break;
        }
      } catch (Exception e) {
        logger.warn("can't retrieve volume information for volume retry ()", retry + 1, e);
        if (retry == maxAttempts) {
          logger.error("cannot get volume metadata after retried {} times", maxAttempts);
        } else {
          logger.warn("sleep 3 seconds");
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger
                .warn("when checking if volume has been extended the coordinator, can`t sleep", e1);
          }
          retry++;
          continue;
        }
      }
    }
  }

  @Override
  public void updateVolumeMetadataIfVolumeExtended() throws Exception {
    int maxAttempts = 10;
    int retry = 0;
    while (true) {
      try {
        SpaceSavingVolumeMetadata volumeMetadata = volumeInfoRetriever.getVolume(this.volumeId);
        if (volumeMetadata == null) {
          throw new InitializationException(
              "Can't retrieve volume metadata to check if volume extended");
        } else {
          if (volumeMetadata.getVolumeSize() == this.volumeMetadata.getVolumeSize()) {
            logger.debug("volume hasn't been extended, just return");
            break;
          } else {
            updateVolumeMetadata(volumeMetadata, this.volumeMetadata.getSegmentCount(),
                volumeMetadata.getSegmentCount() - this.volumeMetadata.getSegmentCount());
            break;
          }
        }
      } catch (Exception e) {
        logger.warn("can't retrieve volume information for volume retry ()", retry + 1, e);
        if (retry == maxAttempts) {
          throw e;
        } else {
          logger.warn("sleep 3 seconds");
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger
                .warn("when checking if volume has been extended the coordinator, can`t sleep", e1);
          }
          retry++;
          continue;
        }
      }
    }
  }

  @Override
  public void setNotifyDatanodeCount(int notifyDatanodeCount) {
    this.notifyDatanodeCount.set(notifyDatanodeCount);
  }

  @Override
  public void decNotifyDatanodeCount() {
    this.notifyDatanodeCount.decrementAndGet();
  }

  @Override
  public boolean allDatanodeResponse() {
    return this.notifyDatanodeCount.get() == 0;
  }

  @Override
  public boolean hasFailedDatanode() {
    return !this.failedDatanode.isEmpty();
  }

  @Override
  public List<InstanceId> pullAllFailedDatanodes() {
    List<InstanceId> allFailedDatanodes = new ArrayList<>();
    this.failedDatanode.drainTo(allFailedDatanodes);
    return allFailedDatanodes;
  }

  @Override
  public void addFailedDatanode(InstanceId failedDatanode) {
    this.failedDatanode.offer(failedDatanode);
  }

  @Override
  public boolean volumeIsAvailable() {
    return volumeIsAvailable(this.volumeMetadata);
  }


  private boolean volumeIsAvailable(SpaceSavingVolumeMetadata volumeMetadata) {
    if (volumeMetadata == null) {
      logger.warn("have not get volumeMetadata:{} from infocenter", this.volumeId);
      return false;
    }
    if (!volumeMetadata.getVolumeStatus().isAvailable()) {
      logger.warn("has retrieved volume metadata, but volume::{} not available, status:{}",
          this.volumeId, volumeMetadata.getVolumeStatus());
      return false;
    }
    if (volumeMetadata.getSegmentTableSize() != volumeMetadata.getSegmentCount()) {
      logger.warn(
          "has retrieved volume:{} metadata, require segments:{} not enough, current segment "
              + "count:{}",
          this.volumeId, volumeMetadata.getSegmentCount(),
          volumeMetadata.getSegmentTableSize());
      return false;
    }
    return true;
  }

  @Override
  public int getLeftNotifyDatanodeCount() {
    return this.notifyDatanodeCount.get();
  }

  @Override
  public Long getVolumeId() {
    return volumeId;
  }

  @Override
  public VolumeInfoRetriever getVolumeInfoRetriever() {
    return volumeInfoRetriever;
  }

  @Override
  public Multimap<Integer, IoUnitContextPacket> getIoContextWaitingForVolumeCompletion() {
    return ioContextWaitingForVolumeCompletion;
  }

  @Override
  public Map<SegId, Integer> getMapSegIdToIndex() {
    return mapSegIdToIndex;
  }

  @Override
  public Map<SegId, AtomicLong> getRecordLastIoTime() {
    return recordLastIoTime;
  }

  @Override
  public SpaceSavingVolumeMetadata getVolumeMetadata() {
    return volumeMetadata;
  }

  @Override
  public LogicalToPhysicalCalculator getLogicalToPhysicalCalculator() {
    return logicalToPhysicalCalculator;
  }

  @Override
  public void setLogicalToPhysicalCalculator(LogicalToPhysicalCalculator calculator) {
    logicalToPhysicalCalculator = calculator;
  }

  @Override
  public StoragePoolThrift getStoragePoolThrift() {
    return storagePoolThrift;
  }
}
