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

package py.coordinator.task;


import py.common.DelayRequest;
import py.volume.CacheType;
import py.volume.VolumeType;


public class CreateSegmentContext extends DelayRequest {

  private Long volumeId;
  private int startSegmentIndex;
  private int segmentCount;
  private VolumeType volumeType;
  private boolean createRequestSent;
  private long domainId;
  private long storagePoolId;
  private CacheType cacheType;



  public CreateSegmentContext(long delay, Long volumeId, int startSegmentIndex,
      VolumeType volumeType, CacheType cacheType,
      int segmentCount, long domainId, long storagePoolId) {
    super(delay);
    this.volumeId = volumeId;
    this.startSegmentIndex = startSegmentIndex;
    this.segmentCount = segmentCount;
    this.volumeType = volumeType;
    this.createRequestSent = false;
    this.domainId = domainId;
    this.storagePoolId = storagePoolId;
    this.cacheType = cacheType;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public void setVolumeType(VolumeType volumeType) {
    this.volumeType = volumeType;
  }

  public boolean isCreateRequestSent() {
    return createRequestSent;
  }

  public void setCreateRequestSent(boolean createRequestSent) {
    this.createRequestSent = createRequestSent;
  }

  public int getStartSegmentIndex() {
    return startSegmentIndex;
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  public long getDomainId() {
    return domainId;
  }

  public void setDomainId(long domainId) {
    this.domainId = domainId;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public CacheType getCacheType() {
    return cacheType;
  }


  public Long getVolumeId() {
    return volumeId;
  }



  public CreateSegmentContext clone() {
    CreateSegmentContext newSegment = new CreateSegmentContext(getDelay(), volumeId,
        startSegmentIndex, volumeType, cacheType,
        segmentCount, domainId, storagePoolId);
    newSegment.setCreateRequestSent(isCreateRequestSent());
    return newSegment;
  }

}
