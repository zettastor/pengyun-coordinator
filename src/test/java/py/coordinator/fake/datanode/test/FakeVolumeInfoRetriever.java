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

package py.coordinator.fake.datanode.test;

import java.util.HashSet;
import java.util.Set;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentVersion;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.share.StoragePoolThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * xx.
 */
public class FakeVolumeInfoRetriever implements VolumeInfoRetriever {

  private static long volumeSizeForTest = 1073741824L;
  private long volumeId;
  private long accountId;

  public FakeVolumeInfoRetriever(long volumeId, long accountId) {
    this.volumeId = volumeId;
    this.accountId = accountId;
  }

  @Override
  public SpaceSavingVolumeMetadata getVolume(Long volumeId) {
    final SegId segId = new SegId(volumeId, 0);
    final int epoch = 1;
    final int generation = 0;
    final InstanceId primary = new InstanceId(1000011);
    Set<InstanceId> secondaries = new HashSet<InstanceId>();
    secondaries.add(new InstanceId(1000012));
    secondaries.add(new InstanceId(1000013));
    // directly return a certain volume meta data
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setAccountId(accountId);
    volumeMetadata.setRootVolumeId(volumeId);
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeSize(volumeSizeForTest);
    // volumeMetaData.getMemberships()
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(epoch, generation),
        primary,
        secondaries);
    // volumeMetaData.getSegmentByIndex(segmentIndex).setLogId(returnMaxLogId)
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());
    volumeMetadata.addSegmentMetadata(segmentMetadata, membership);
    // volumeMetaData.getSegmentByIndex(segmentIndex).getSegId()
    return new SpaceSavingVolumeMetadata(volumeMetadata);
  }

  @Override
  public boolean createSegment(Long volumeId, int startSegmentIndex, VolumeType volumeType,
      CacheType cacheType, int segmentCount,
      long domainId, long storagePoolId) throws Exception {
    // TODO Auto-generated method stub
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
