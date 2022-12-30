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
import py.volume.VolumeType;


public interface VolumeInfoRetriever {

  public SpaceSavingVolumeMetadata getVolume(Long volumeId) throws Exception;

  public boolean createSegment(Long volumeId, int startSegmentIndex, VolumeType volumeType,
      CacheType cacheType, int segmentCount,
      long domainId, long storagePoolId) throws Exception;

  public StoragePoolThrift getStoragePoolInfo(long domainId, long storagePoolId) throws Exception;

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory);

}
