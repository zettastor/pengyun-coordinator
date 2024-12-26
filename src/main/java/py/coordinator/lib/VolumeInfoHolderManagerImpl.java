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

package py.coordinator.lib;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.CloneType;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;


public class VolumeInfoHolderManagerImpl implements VolumeInfoHolderManager {

  protected static final Logger logger = LoggerFactory.getLogger(VolumeInfoHolderManagerImpl.class);
  private Map<Long, VolumeInfoHolder> volumeInfoHolderMap;


  private Map<Long, Long> mapVolumeIdToRootVolumeId;

  private VolumeInfoRetriever volumeInfoRetriever;



  public VolumeInfoHolderManagerImpl(VolumeInfoRetriever volumeInfoRetriever) {
    this.volumeInfoHolderMap = new ConcurrentHashMap<>();
    this.mapVolumeIdToRootVolumeId = new ConcurrentHashMap<>();
    this.volumeInfoRetriever = volumeInfoRetriever;
  }


  @Override
  public VolumeInfoHolder getVolumeInfoHolder(Long rootVolumeId) {
    VolumeInfoHolder volumeInfoHolder;


    volumeInfoHolder = volumeInfoHolderMap.get(rootVolumeId);

    if (volumeInfoHolder == null) {
      Long mappedRootVolumeId = mapVolumeIdToRootVolumeId.get(rootVolumeId);
      if (mappedRootVolumeId == null) {

        try {
          SpaceSavingVolumeMetadata volumeMetadata = volumeInfoRetriever.getVolume(rootVolumeId);
          mapVolumeIdToRootVolumeId.putIfAbsent(rootVolumeId, volumeMetadata.getRootVolumeId());
          logger.error("user pass volumeId:{} not root volumeId:{}", rootVolumeId,
              volumeMetadata.getRootVolumeId());
        } catch (Exception e) {
          logger.error("caught an exception when get volume info:{}", rootVolumeId, e);
          throw new RuntimeException();
        }
      }

      volumeInfoHolder = volumeInfoHolderMap.get(mappedRootVolumeId);
      Validate.notNull(volumeInfoHolder);
    }

    return volumeInfoHolder;
  }

  @Override
  public void removeVolumeInfoHolder(Long rootVolumeId) {
    this.volumeInfoHolderMap.remove(rootVolumeId);
  }

  @Override
  public void addVolumeInfoHolder(Long rootVolumeId, int snapshotId) throws Exception {
    do {
      if (containVolumeInfoHolder(rootVolumeId)) {
        logger.warn("no need init volumeId:{}, snapshotId again", rootVolumeId);
        return;
      }

      VolumeInfoHolder volumeHolder = new VolumeInfoHolderImpl(rootVolumeId, snapshotId,
          volumeInfoRetriever, mapVolumeIdToRootVolumeId);

      try {
        volumeHolder.init();
        volumeInfoHolderMap.putIfAbsent(rootVolumeId, volumeHolder);
      } catch (Exception e) {
        logger.error("caught an exception when init volume holder:{}", rootVolumeId, e);
        throw e;
      }


      SpaceSavingVolumeMetadata volumeMetadata = volumeHolder.getVolumeMetadata();
      if (volumeMetadata.getCloneType() == CloneType.LINKED_CLONE) {
        rootVolumeId = volumeMetadata.getSourceVolumeId();
        snapshotId = volumeMetadata.getSourceSnapshotId();

        logger.info("a linked clone volume {} need get his source volume metadata",
            volumeMetadata.getVolumeId());
        continue;
      }
      break;
    } while (true);

    return;
  }

  @Override
  public void addVolumeInfoHolder(Long rootVolumeId, VolumeInfoHolder volumeInfoHolder) {
    this.volumeInfoHolderMap.putIfAbsent(rootVolumeId, volumeInfoHolder);
  }

  @Override
  public boolean containVolumeInfoHolder(Long rootVolumeId) {
    return volumeInfoHolderMap.containsKey(rootVolumeId);
  }

  @Override
  public Collection<VolumeInfoHolder> getAllVolumeInfoHolder() {
    return this.volumeInfoHolderMap.values();
  }

  @Override
  public Map<Long, Long> getRootVolumeIdMap() {
    return this.mapVolumeIdToRootVolumeId;
  }

  @Override
  public void setVolumeInfoRetrieve(VolumeInfoRetriever volumeInfoRetriever) {
    this.volumeInfoRetriever = volumeInfoRetriever;
  }
}
