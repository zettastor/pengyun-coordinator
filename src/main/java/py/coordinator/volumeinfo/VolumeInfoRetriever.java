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

package py.coordinator.volumeinfo;

import py.infocenter.client.InformationCenterClientFactory;
import py.thrift.share.StoragePoolThrift;
import py.volume.CacheType;
import py.volume.VolumeType;


public interface VolumeInfoRetriever {

  public SpaceSavingVolumeMetadata getVolume(Long volumeId) throws Exception;

  public boolean createSegment(Long volumeId, int startSegmentIndex, VolumeType volumeType,
      CacheType cacheType, int segmentCount,
      long domainId, long storagePoolId) throws Exception;

  public StoragePoolThrift getStoragePoolInfo(long domainId, long storagePoolId) throws Exception;

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory);

}
