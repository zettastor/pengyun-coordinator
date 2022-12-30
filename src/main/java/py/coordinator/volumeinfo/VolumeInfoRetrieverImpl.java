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

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.thrift.infocenter.service.CreateSegmentsRequest;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.ListStoragePoolResponseThrift;
import py.thrift.share.OneStoragePoolDisplayThrift;
import py.thrift.share.StoragePoolNotExistedExceptionThrift;
import py.thrift.share.StoragePoolThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;


public class VolumeInfoRetrieverImpl implements VolumeInfoRetriever {

  private static final Logger logger = LoggerFactory.getLogger(VolumeInfoRetrieverImpl.class);
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
  private static final long accountId = SUPERADMIN_ACCOUNT_ID;
  private InformationCenterClientFactory infoCenterClientFactory;
  private int timeout = 0;

  public VolumeInfoRetrieverImpl(int timeout) {
    this.timeout = timeout;
  }

  public VolumeInfoRetrieverImpl() {
    this(DEFAULT_REQUEST_TIMEOUT_MS);
  }

  @Override
  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }


  @Override
  public SpaceSavingVolumeMetadata getVolume(Long volumeId) throws Exception {
    try {
      InformationCenterClientWrapper client = infoCenterClientFactory.build(timeout);
      logger.warn("try to get info center client in time: {}", timeout);
      VolumeMetadata volumeMetadata = client.getVolumeByPagination(volumeId, accountId)
          .getVolumeMetadata();
      SpaceSavingVolumeMetadata spaceSavingVolumeMetadata = new SpaceSavingVolumeMetadata(
          volumeMetadata);
      return spaceSavingVolumeMetadata;
    } catch (Exception e) {
      logger.warn("catch an exception when get volume: {}, accountId: {}", volumeId, accountId, e);
      throw e;
    }
  }

  @Override
  public StoragePoolThrift getStoragePoolInfo(long domainId, long storagePoolId) throws Exception {
    ListStoragePoolRequestThrift requestThrift = new ListStoragePoolRequestThrift();
    requestThrift.setRequestId(RequestIdBuilder.get());
    requestThrift.setAccountId(accountId);
    requestThrift.setDomainId(domainId);
    List<Long> storagePoolIdList = Arrays.asList(storagePoolId);
    requestThrift.setStoragePoolIds(storagePoolIdList);
    try {
      InformationCenterClientWrapper client = infoCenterClientFactory.build(timeout);
      logger.warn("try to get info center client in time: {}", timeout);
      ListStoragePoolResponseThrift listStoragePoolResponseThrift = client.getClient()
          .listStoragePools(requestThrift);
      List<OneStoragePoolDisplayThrift> storagePoolDisplayList = listStoragePoolResponseThrift
          .getStoragePoolDisplays();
      if (storagePoolDisplayList.size() == 1) {
        OneStoragePoolDisplayThrift oneStoragePoolDisplayThrift = storagePoolDisplayList.get(0);
        StoragePoolThrift storagePoolThrift = oneStoragePoolDisplayThrift.getStoragePoolThrift();
        logger.warn("Get StoragePoolThrift:{} ", storagePoolThrift);
        return storagePoolThrift;
      } else {
        logger.error("storagePoolDisplayList size is {}. when get domainId: {}, storagePoolId: {}",
            storagePoolDisplayList.size(), domainId,
            storagePoolId);
        throw new StoragePoolNotExistedExceptionThrift();
      }
    } catch (Exception e) {
      logger.error("catch an exception when get domainId: {}, storagePoolId: {}", domainId,
          storagePoolId, e);
      throw e;
    }
  }

  public long getAccountId() {
    return accountId;
  }

  @Override
  public boolean createSegment(Long volumeId, int startSegmentIndex, VolumeType volumeType,
      CacheType cacheType, int segmentCount,
      long domainId, long storagePoolId) {
    try {
      logger.warn("try to create segment {} for a uncompleted volume, accountid {}",
          startSegmentIndex,
          accountId);
      InformationCenter.Iface client = infoCenterClientFactory.build().getClient();
      CreateSegmentsRequest request = new CreateSegmentsRequest(RequestIdBuilder.get(), volumeId,
          startSegmentIndex, segmentCount, volumeType.getVolumeTypeThrift(),
          cacheType.getCacheTypeThrift(),
          accountId, domainId, storagePoolId);
      client.createSegments(request);
      return true;
    } catch (Exception e) {
      logger.error("error while creating segment", e);
      return false;
    }
  }

}
