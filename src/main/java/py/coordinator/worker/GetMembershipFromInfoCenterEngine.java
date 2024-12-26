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

package py.coordinator.worker;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.coordinator.lib.Coordinator;
import py.coordinator.task.SingleTask;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.membership.SegmentMembership;


public class GetMembershipFromInfoCenterEngine extends BaseKeepSingleTaskEngine {

  private static final Logger logger = LoggerFactory
      .getLogger(GetMembershipFromInfoCenterEngine.class);

  private final Coordinator coordinator;

  public GetMembershipFromInfoCenterEngine(String threadName, Coordinator coordinator) {
    super(threadName);
    this.coordinator = coordinator;
  }


  @Override
  public void process(List<SingleTask> singleTasks) {
    Validate.notEmpty(singleTasks);
   
    SpaceSavingVolumeMetadata updatedVolumeMetadata = null;
    Multimap<Long, SingleTask> taskMultiMap = HashMultimap.create();
    for (SingleTask singleTask : singleTasks) {
      taskMultiMap.put(singleTask.getVolumeId(), singleTask);
    }

    for (Map.Entry<Long, Collection<SingleTask>> entry : taskMultiMap.asMap().entrySet()) {
      Long volumeId = entry.getKey();
      Collection<SingleTask> taskInOneVolume = entry.getValue();
      try {
        VolumeInfoRetriever volumeInfoRetriever = coordinator.getVolumeInfoRetriever();
        updatedVolumeMetadata = volumeInfoRetriever.getVolume(volumeId);
      } catch (Exception e) {
        logger.error("can not get volume from infocenter at:{}", singleTasks);
      }

      if (updatedVolumeMetadata != null) {
        for (SingleTask singleTask : taskInOneVolume) {
          try {
            SegId segId = (SegId) singleTask.getCompareKey();
            int segmentIndex = coordinator.getSegmentIndex(volumeId, segId);
            SegmentMembership highestMembership = updatedVolumeMetadata
                .getHighestMembership(segmentIndex);
            if (highestMembership != null) {
              coordinator
                  .forceUpdateMembershipFromDatanode(volumeId, (SegId) singleTask.getCompareKey(),
                      highestMembership, singleTask.getRequestId());
            }
          } catch (Exception e) {
            logger.error("failed to process task:{}", singleTask, e);
          } finally {
            freeTask(singleTask);
          }
        }
      } else {
        logger.error("can not get volume for segId: {}", singleTasks);
      }
    }
  }
}
