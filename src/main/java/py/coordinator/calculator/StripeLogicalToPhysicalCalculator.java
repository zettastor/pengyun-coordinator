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

package py.coordinator.calculator;

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.struct.Pair;
import py.coordinator.nbd.Util;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.drivercontainer.exception.ProcessPositionException;
import py.exception.StorageException;


public class StripeLogicalToPhysicalCalculator implements LogicalToPhysicalCalculator {

  public static final long MAX_GET_VOLUME_TIMEOUT_MS = 60000;
  private static final Logger logger = LoggerFactory
      .getLogger(StripeLogicalToPhysicalCalculator.class);
  private final VolumeInfoRetriever volumeInfoRetriever;
  private final Long volumeId;
  private final long segmentSize;
  private final long pageSize;
  private int pagePackageCount;

  private volatile List<Long> volumeLayout;



  public StripeLogicalToPhysicalCalculator(VolumeInfoRetriever volumeInfoRetriever, Long volumeId,
      long segmentSize, int pageSize) {
    this.volumeInfoRetriever = volumeInfoRetriever;
    this.volumeId = volumeId;
    this.segmentSize = segmentSize;
    this.pageSize = pageSize;
    this.volumeLayout = new ArrayList<>();
  }



  public static List<Long> eachTimeExtendVolumeSize2SizeList(String eachTimeExtendVolumeSize)
      throws StorageException {
    List<Long> poolIdList = new ArrayList<>();
    if (eachTimeExtendVolumeSize == null || eachTimeExtendVolumeSize.isEmpty()) {
      return poolIdList;
    }

    String[] poolIdArray = eachTimeExtendVolumeSize.split(",");
    for (String poolIdTemp : poolIdArray) {
      if (poolIdTemp == null || poolIdTemp.isEmpty()) {
        continue;
      }
      long poolId;
      try {
        poolId = Long.valueOf(poolIdTemp);
      } catch (Exception e) {
        logger.error("caught a exception when eachTimeExtendVolumeSize size list,", e);
        throw new StorageException();
      }
      poolIdList.add(poolId);
    }

    return poolIdList;
  }


  public void initPagePackageCount(int pagePackageCount) {
    this.pagePackageCount = pagePackageCount;
  }


  public List<Long> getVolumeLayout() {
    return volumeLayout;
  }


  public void setVolumeLayout(List<Long> volumeLayout) {
    this.volumeLayout = volumeLayout;
  }

  public int calculateIndex(long position) {
    return (int) (position / segmentSize);
  }


  @Override
  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position)
      throws ProcessPositionException, StorageException {
    try {
      return Util.processConvertPos(position, pageSize, segmentSize, pagePackageCount, volumeLayout,
          false);
    } catch (ProcessPositionException e) {
      updateVolumeInformation();
      return Util.processConvertPos(position, pageSize, segmentSize, pagePackageCount, volumeLayout,
          false);
    }
  }

  @Override
  public Pair<Integer, Long> convertLogicalPositionToPhysical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException {
    long wholeCostTimeMs = 0L;
    do {
      long startTime = System.currentTimeMillis();
      try {
        return convertLogicalPositionToPhysical(position);
      } catch (ProcessPositionException | StorageException e) {
        logger.warn("caught an exception", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          logger.warn("try to sleep caught an exception", e);
        }
        long singleCostTimeMs = System.currentTimeMillis() - startTime;
        wholeCostTimeMs += singleCostTimeMs;
        continue;
      }
    } while (wholeCostTimeMs < timeoutMs);
    throw new ProcessPositionException();
  }

  @Override
  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position)
      throws ProcessPositionException, StorageException {
    try {
      return Util
          .processConvertPos(position, pageSize, segmentSize, pagePackageCount, volumeLayout, true);
    } catch (ProcessPositionException e) {
      updateVolumeInformation();
      return Util
          .processConvertPos(position, pageSize, segmentSize, pagePackageCount, volumeLayout, true);
    }
  }

  @Override
  public Pair<Integer, Long> convertPhysicalPositionToLogical(long position, long timeoutMs)
      throws ProcessPositionException, StorageException {
    long wholeCostTimeMs = 0L;
    do {
      long startTime = System.currentTimeMillis();
      try {
        return convertPhysicalPositionToLogical(position);
      } catch (ProcessPositionException | StorageException e) {
        logger.warn("caught an exception", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          logger.warn("try to sleep caught an exception", e);
        }
        long singleCostTimeMs = System.currentTimeMillis() - startTime;
        wholeCostTimeMs += singleCostTimeMs;
        continue;
      }
    } while (wholeCostTimeMs < timeoutMs);
    throw new ProcessPositionException();
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public long getPageSize() {
    return pageSize;
  }

  private void getVolumeMapAndList(SpaceSavingVolumeMetadata volumeMetadata,
      List<Long> volumeListInOrder, Multimap<Long, Long> recordSegmentCount) {
    Long volumeId = -1L;
    int allSegmentCount = volumeMetadata.getSegmentCount();
    for (int index = 0; index < allSegmentCount; index++) {
      SegId segId = volumeMetadata.getSegId(index);
      if (segId != null) {
        Long currentVolumeId = segId.getVolumeId().getId();
        if (!volumeId.equals(currentVolumeId)) {
          volumeId = currentVolumeId;
          volumeListInOrder.add(volumeId);
        }
        recordSegmentCount.put(currentVolumeId, (long) index);
      }
    }
    logger.warn("volume id :{}, map.key:{}", volumeListInOrder, recordSegmentCount.keySet());
  }

  private void getVolumeMapAndList(SpaceSavingVolumeMetadata volumeMetadata,
      List<Long> eachTimeExtendVolumeSize,
      List<Long> volumeListInOrder, Map<Long, Long> recordSegmentCount) {

    for (int i = 0; i < eachTimeExtendVolumeSize.size(); i++) {
      recordSegmentCount.put(Long.valueOf(i), eachTimeExtendVolumeSize.get(i));
    }
    logger.warn("the volume id :{}, and record Segment Count:{}", volumeMetadata.getVolumeId(),
        recordSegmentCount);
  }



  public void updateVolumeInformation() throws StorageException {
    try {
      SpaceSavingVolumeMetadata volumeMetadata = null;
      int tryMaxCount = 50;
      int tryTime = 0;
      long sleepUnitMs = 3000;
      boolean continueProcess = false;
      int segmentWrappedCount = 0;
      List<Long> volumeListInOrder = new ArrayList<Long>();
      Map<Long, Long> mapSegmentCount = new ConcurrentHashMap<>();

      List<Long> eachTimeExtendVolumeSize = new ArrayList<Long>();
      while (tryTime++ < tryMaxCount) {
        try {
          volumeMetadata = volumeInfoRetriever.getVolume(this.volumeId);
          if (volumeMetadata != null) {
            segmentWrappedCount = volumeMetadata.getSegmentWrappCount();
            volumeListInOrder.clear();
            mapSegmentCount.clear();
            int expectSegmentCount = volumeMetadata.getSegmentCount();
            int actualSegmentCount = volumeMetadata.getRealSegmentCount();
            if (actualSegmentCount == expectSegmentCount) {
              eachTimeExtendVolumeSize = eachTimeExtendVolumeSize2SizeList(
                  volumeMetadata.getEachTimeExtendVolumeSize());
              getVolumeMapAndList(volumeMetadata, eachTimeExtendVolumeSize, volumeListInOrder,
                  mapSegmentCount);

              logger.warn("volume:{} got enough segments:{}", volumeMetadata.getVolumeId(),
                  actualSegmentCount);
              continueProcess = true;
              break;
            } else {
              logger.warn("volume:{} does not have enough segments:{}, expect segment count:{}",
                  volumeMetadata.getVolumeId(), actualSegmentCount, expectSegmentCount);
            }
          }
        } catch (Exception e) {
          Thread.sleep(sleepUnitMs);
          logger.warn("try to get volume info at:{} time", tryTime, e);
        }
      }

      if (!continueProcess) {
        logger.error("can not get volume info after try some times:{}", tryMaxCount);
        throw new StorageException();
      }

      this.pagePackageCount = volumeMetadata.getPageWrappCount();

      List<Long> tmp = new ArrayList<Long>();
      for (long volumeIdInOrder = 0; volumeIdInOrder < mapSegmentCount.keySet().size();
          volumeIdInOrder++) {
        Long currentSegmentCount = (long) mapSegmentCount.get(volumeIdInOrder);
        logger.warn("volume id:{} has {} segment count", volumeIdInOrder, currentSegmentCount);
        if (segmentWrappedCount > 0) {
          for (int splitCount = 0; ; splitCount++) {
            if (currentSegmentCount > 0) {
              long currentAddLayout;
              if (currentSegmentCount >= segmentWrappedCount) {
                currentSegmentCount -= segmentWrappedCount;
                currentAddLayout = segmentWrappedCount;
                tmp.add(currentAddLayout);
              } else {
                currentAddLayout = currentSegmentCount;
                currentSegmentCount = 0L;
                tmp.add(currentAddLayout);
              }
              logger.warn("volume id:{} {} time has [{}] layout", volumeIdInOrder, splitCount,
                  currentAddLayout);
            } else {
              break;
            }
          }
        } else {
          tmp.add(currentSegmentCount);
        }
      }
      volumeLayout = tmp;
    } catch (Exception e) {
      logger.error("can not update volume information", e);
      throw new StorageException();
    }
  }

  /*
   * calculate segment layout of simple volume
   */
  private List<Long> updateSimpleVolumeInformation(SpaceSavingVolumeMetadata volumeMetadata,
      List<Long> volumeListInOrder, Map<Long, Long> mapSegmentCount) {
    int segmentNumToCreateEachTime = volumeMetadata.getSegmentNumToCreateEachTime();

    Long rootVolumeId = volumeMetadata.getRootVolumeId();
    int eachTimeCount = 0;
    final int rootVolumeSegCount;
    long wrappedCount = 0;
    List<Long> simpleVolumeLayout = new ArrayList<>();
    int segmentWrappedCount = volumeMetadata.getSegmentWrappCount();

    logger
        .warn("updateSimpleVolumeInformation segmentNumToCreateEachTime :{} segmentWrappedCount:{}",
            segmentNumToCreateEachTime, segmentWrappedCount);


    if (segmentNumToCreateEachTime < segmentWrappedCount) {
      logger.warn("segmentNumToCreateEachTime:{} less than segmentWrappedCount:{}",
          segmentNumToCreateEachTime, segmentWrappedCount);
      segmentWrappedCount = segmentNumToCreateEachTime;
    }

    if (segmentWrappedCount == 0) {


      Validate.isTrue(false, "segmentWrappedCount can not be zero" + volumeMetadata);
    }




    rootVolumeSegCount = mapSegmentCount.get(0L).intValue();
    logger.warn("root volume segment count:{} . total volume segment count {} ", rootVolumeSegCount,
        mapSegmentCount.size());

    int expectSegmentCount = volumeMetadata.getSegmentCount();
    int actualSegmentCount = volumeMetadata.getRealSegmentCount();


    for (int index = 0; index < rootVolumeSegCount; index++) {
      wrappedCount++;
      eachTimeCount++;
      if (wrappedCount == segmentWrappedCount) {


        simpleVolumeLayout.add(wrappedCount);
        wrappedCount = 0; // reset to zero
      }
      if (eachTimeCount == segmentNumToCreateEachTime) {

        eachTimeCount = 0;
        if (wrappedCount > 0) {
          simpleVolumeLayout.add(wrappedCount);
          wrappedCount = 0;
        }
      }
    }

    if (expectSegmentCount == actualSegmentCount) {
      if (wrappedCount != 0) {

        logger.warn("wrappedCount :{} segmentWrappedCount:{} segmentNumToCreateEachTime:{}",
            wrappedCount, segmentWrappedCount, segmentNumToCreateEachTime);
        simpleVolumeLayout.add(wrappedCount);
      }
    }



    for (long volumeIdInOrder = 1; volumeIdInOrder < mapSegmentCount.keySet().size();
        volumeIdInOrder++) {
      Long currentSegmentCount = (long) mapSegmentCount.get(volumeIdInOrder);
      logger.warn("volume id:{} has {} segment count", volumeIdInOrder, currentSegmentCount);
      for (int splitCount = 0; ; splitCount++) {
        if (currentSegmentCount > 0) {
          long currentAddLayout;
          if (currentSegmentCount >= segmentWrappedCount) {
            currentSegmentCount -= segmentWrappedCount;
            currentAddLayout = segmentWrappedCount;
            simpleVolumeLayout.add(currentAddLayout);
          } else {
            currentAddLayout = currentSegmentCount;
            currentSegmentCount = 0L;
            simpleVolumeLayout.add(currentAddLayout);
          }
          logger.warn("volume id:{} {} time has [{}] layout", volumeIdInOrder, splitCount,
              currentAddLayout);
        } else {
          break;
        }
      }
    }

    logger.warn("simpleVolumeLayout:{}", simpleVolumeLayout);
    return simpleVolumeLayout;
  }

}
