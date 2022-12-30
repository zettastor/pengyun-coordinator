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
