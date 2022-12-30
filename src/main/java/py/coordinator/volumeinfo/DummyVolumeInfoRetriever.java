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

import py.infocenter.client.InformationCenterClientFactory;
import py.thrift.share.StoragePoolThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;


public class DummyVolumeInfoRetriever implements VolumeInfoRetriever {

  private SpaceSavingVolumeMetadata volumeMetadata;

  public DummyVolumeInfoRetriever() {

  }



  public DummyVolumeInfoRetriever(VolumeMetadata volumeMetadata) {
    if (null != volumeMetadata) {
      this.volumeMetadata = new SpaceSavingVolumeMetadata(volumeMetadata);
    }
  }

  public DummyVolumeInfoRetriever(SpaceSavingVolumeMetadata volumeMetadata) {
    this.volumeMetadata = volumeMetadata;
  }

  @Override
  public SpaceSavingVolumeMetadata getVolume(Long volumeId) {
    return volumeMetadata;
  }

  @Override
  public boolean createSegment(Long volumeId, int startSegmentIndex, VolumeType volumeType,
      CacheType cacheType, int segmentCount,
      long domainId, long storagePoolId) throws Exception {

    return false;
  }


  @Override
  public StoragePoolThrift getStoragePoolInfo(long domainId, long storagePoolId) {
    return null;
  }

  @Override
  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {

  }

}
