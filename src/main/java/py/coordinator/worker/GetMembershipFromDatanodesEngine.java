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

import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.lib.Coordinator;
import py.coordinator.response.GetMembershipCallbackCollector;
import py.coordinator.response.GetMembershipFromDatanodeCallback;
import py.coordinator.task.GetMembershipTask;
import py.coordinator.task.SingleTask;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;


public class GetMembershipFromDatanodesEngine extends BaseKeepSingleTaskEngine {

  private static final Logger logger = LoggerFactory
      .getLogger(GetMembershipFromDatanodesEngine.class);

  private final Coordinator coordinator;

  public GetMembershipFromDatanodesEngine(String threadName, Coordinator coordinator) {
    super(threadName);
    this.coordinator = coordinator;
  }

  @Override
  public void process(List<SingleTask> singleTasks) {
    Validate.notEmpty(singleTasks);


    for (SingleTask singleTask : singleTasks) {
      try {
        SegmentMembership currentMembership = ((GetMembershipTask) singleTask)
            .getSegmentMembership();
        Validate.notNull(currentMembership);
        SegId segId = (SegId) singleTask.getCompareKey();
        List<EndPoint> endpoints = null;

        try {
          endpoints = RequestResponseHelper
              .buildEndPoints(coordinator.getInstanceStore(), currentMembership, PortType.IO, true,
                  null);
        } catch (Exception e) {
          logger.error("failed to get endpoints by:{}", currentMembership, e);
        }

        if (endpoints != null && !endpoints.isEmpty()) {

          Broadcastlog.PbGetMembershipRequest.Builder builder = Broadcastlog.PbGetMembershipRequest
              .newBuilder();
          builder.setRequestId(RequestIdBuilder.get());
          builder.setVolumeId(segId.getVolumeId().getId());
          builder.setSegIndex(segId.getIndex());
          Broadcastlog.PbGetMembershipRequest request = builder.build();
          GetMembershipFromDatanodeCallback callback = new GetMembershipFromDatanodeCallback(
              singleTask.getVolumeId());
          GetMembershipCallbackCollector getMembershipCallbackCollector =
              new GetMembershipCallbackCollector(
                  this.coordinator, endpoints.size(), segId, singleTask.getRequestId(), callback);
          try {
            for (EndPoint endPoint : endpoints) {
              coordinator.getClientFactory().generate(endPoint)
                  .getMembership(request, getMembershipCallbackCollector);
            }
          } catch (Exception e) {
            logger.error("can not send get membership request to:{}", endpoints, e);
          }
        }
      } finally {
        freeTask(singleTask);
      }
    }
  }

}
