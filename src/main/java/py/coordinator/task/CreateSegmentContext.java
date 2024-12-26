/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
