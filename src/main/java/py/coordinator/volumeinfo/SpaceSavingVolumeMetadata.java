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

package py.coordinator.volumeinfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.struct.LimitQueue;
import py.membership.SegmentMembership;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeMetadata.ReadWriteType;
import py.volume.VolumeMetadata.VolumeSourceType;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class SpaceSavingVolumeMetadata {

  private static final Logger logger = LoggerFactory.getLogger(SpaceSavingVolumeMetadata.class);
  private static final int DEFAULT_STORE_HISTORY_OF_SEGMENT_MEMBERSHIP = 3;
  private long rootVolumeId;
  private long volumeId;
  private long volumeSize;


  private long extendingSize;

  private Long childVolumeId;
  private String name;
  private int positionOfFirstSegmentInLogicVolume;





  private int version;
  private Long domainId;
  private Long storagePoolId;

  private int segmentNumToCreateEachTime;


  private long segmentSize;


  private Map<Integer, LimitQueue<SegmentMembership>> memberships = new ConcurrentHashMap<>();

  private String volumeLayoutString;

  private VolumeType volumeType;

  private CacheType cacheType;

  private CloneType cloneType = CloneType.NONE;

  private volatile VolumeStatus volumeStatus;

  private long deadTime;

  private double freeSpaceRatio = 1.0;

  private VolumeSourceType volumeSource;

  private ReadWriteType readWrite;

  private int pageWrappCount;

  private int segmentWrappCount;

  private boolean enableLaunchMultiDrivers;


  private long sourceVolumeId;
  private int sourceSnapshotId;

  private Map<Integer, SegId> segIdMap;



  private String eachTimeExtendVolumeSize;

  public SpaceSavingVolumeMetadata() {
  }



  public SpaceSavingVolumeMetadata(VolumeMetadata volumeMetadata) {

    this.rootVolumeId = volumeMetadata.getRootVolumeId();
    this.volumeId = volumeMetadata.getVolumeId();
    this.volumeSize = volumeMetadata.getVolumeSize();
    this.extendingSize = volumeMetadata.getExtendingSize();
    this.childVolumeId = volumeMetadata.getChildVolumeId();
    this.name = volumeMetadata.getName();
    this.positionOfFirstSegmentInLogicVolume = volumeMetadata
        .getPositionOfFirstSegmentInLogicVolume();
    this.version = volumeMetadata.getVersion();
    this.domainId = volumeMetadata.getDomainId();
    this.storagePoolId = volumeMetadata.getStoragePoolId();

    this.segmentNumToCreateEachTime = volumeMetadata.getSegmentNumToCreateEachTime();

    this.segmentSize = volumeMetadata.getSegmentSize();

    this.volumeLayoutString = volumeMetadata.getVolumeLayout();

    this.volumeType = volumeMetadata.getVolumeType();

    this.cloneType = CloneType.NONE;
    this.sourceVolumeId = 0;
    this.sourceSnapshotId = 0;

    this.eachTimeExtendVolumeSize = volumeMetadata.getEachTimeExtendVolumeSize();
    if (volumeMetadata.getSegmentTableSize() > 0) {
      SegmentMetadata segmentMetadata = volumeMetadata.getSegmentTable().entrySet().iterator()
          .next().getValue();
      if (segmentMetadata.getSegmentUnitCount() > 0) {
        for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
          if (!segmentUnitMetadata.isArbiter()) {
            this.sourceVolumeId = segmentUnitMetadata.getSrcVolumeId();
          }
        }
      }
    }

    this.volumeStatus = volumeMetadata.getVolumeStatus();

    this.deadTime = volumeMetadata.getDeadTime();

    this.freeSpaceRatio = volumeMetadata.getFreeSpaceRatio();

    this.volumeSource = volumeMetadata.getVolumeSource();

    this.readWrite = volumeMetadata.getReadWrite();

    this.pageWrappCount = volumeMetadata.getPageWrappCount();

    this.segmentWrappCount = volumeMetadata.getSegmentWrappCount();

    this.enableLaunchMultiDrivers = volumeMetadata.isEnableLaunchMultiDrivers();

    this.memberships = new ConcurrentHashMap<>();

    segIdMap = new HashMap<>();
    for (SegmentMetadata segmentMetadata : volumeMetadata.getSegments()) {
      if (segmentMetadata.getSegmentStatus().available()) {
        segIdMap.put(segmentMetadata.getIndex(), segmentMetadata.getSegId());
        SegmentMembership highestMembership = null;
        for (SegmentUnitMetadata segUnit : segmentMetadata.getSegmentUnits()) {
          SegmentMembership currentMembership = segUnit.getMembership();
          if (null == highestMembership
              || currentMembership.compareVersion(highestMembership) > 0) {
            highestMembership = currentMembership;
          }
        }

        addMembership(segmentMetadata.getIndex(), highestMembership);
      }
    }
  }

  public long getRootVolumeId() {
    return rootVolumeId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  public long getExtendingSize() {
    return extendingSize;
  }

  public Long getChildVolumeId() {
    return childVolumeId;
  }

  public String getName() {
    return name;
  }

  public int getPositionOfFirstSegmentInLogicVolume() {
    return positionOfFirstSegmentInLogicVolume;
  }

  public int getVersion() {
    return version;
  }

  public int getSegmentNumToCreateEachTime() {
    return segmentNumToCreateEachTime;
  }

  public void setSegmentNumToCreateEachTime(int segmentNumToCreateEachTime) {
    this.segmentNumToCreateEachTime = segmentNumToCreateEachTime;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public Map<Integer, LimitQueue<SegmentMembership>> getMemberships() {
    return memberships;
  }

  public String getVolumeLayoutString() {
    return volumeLayoutString;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public CacheType getCacheType() {
    return cacheType;
  }

  public CloneType getCloneType() {
    return cloneType;
  }

  public VolumeStatus getVolumeStatus() {
    return volumeStatus;
  }

  public long getDeadTime() {
    return deadTime;
  }

  public double getFreeSpaceRatio() {
    return freeSpaceRatio;
  }

  public VolumeSourceType getVolumeSource() {
    return volumeSource;
  }

  public ReadWriteType getReadWrite() {
    return readWrite;
  }

  public int getPageWrappCount() {
    return pageWrappCount;
  }

  public int getSegmentWrappCount() {
    return segmentWrappCount;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public long getSourceVolumeId() {
    return sourceVolumeId;
  }

  public int getSourceSnapshotId() {
    return sourceSnapshotId;
  }

  public Long getDomainId() {
    return domainId;
  }

  public Long getStoragePoolId() {
    return storagePoolId;
  }

  public int getSegmentTableSize() {
    return segIdMap.size();
  }

  public int getRealSegmentCount() {
    return getSegmentTableSize();
  }

  public int getSegmentCount() {
    return (int) (Math.ceil(volumeSize * 1.0 / segmentSize));
  }

  public SegId getSegId(int index) {
    return segIdMap.get(index);
  }

  public SegmentMembership getHighestMembership(int index) {
    LimitQueue<SegmentMembership> limitQueue = memberships.get(index);
    return limitQueue == null ? null : limitQueue.getLast();
  }



  public void addMembership(int segIndex, SegmentMembership highestMembershipInSegment) {
    LimitQueue<SegmentMembership> segmentMemberships = memberships.get(segIndex);
    if (segmentMemberships == null) {
      segmentMemberships = new LimitQueue(DEFAULT_STORE_HISTORY_OF_SEGMENT_MEMBERSHIP);
      memberships.put(segIndex, segmentMemberships);
    }
    segIdMap.putIfAbsent(segIndex, new SegId(volumeId, segIndex));
    segmentMemberships.offer(highestMembershipInSegment);
  }



  public void updateMembership(int segIndex, SegmentMembership membership) {
    LimitQueue<SegmentMembership> segmentMemberships = memberships.get(segIndex);
    if (segmentMemberships == null) {
      String errMsg = "segment " + segIndex + " doesn't exist in the membership table";
      logger.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    segmentMemberships.offer(membership);
  }

  public String getEachTimeExtendVolumeSize() {
    return eachTimeExtendVolumeSize;
  }
}
