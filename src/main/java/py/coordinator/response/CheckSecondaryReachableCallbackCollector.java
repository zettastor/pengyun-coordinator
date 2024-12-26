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

package py.coordinator.response;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.coordinator.lib.Coordinator;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.SegmentMembership;
import py.netty.exception.DisconnectionException;
import py.netty.exception.IncompleteGroupException;
import py.netty.exception.NetworkUnhealthyException;
import py.netty.exception.SegmentDeleteException;
import py.netty.exception.SegmentNotFoundException;
import py.netty.exception.ServerPausedException;
import py.netty.exception.ServerShutdownException;
import py.proto.Broadcastlog;


public class CheckSecondaryReachableCallbackCollector extends CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(CheckSecondaryReachableCallbackCollector.class);
  private boolean caughtIncompleteGroupException;



  public CheckSecondaryReachableCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList, SimpleInstance checkInstance, SegId segId,
      TriggerByCheckCallback ioMethodCallback, SegmentMembership membershipWhenIoCome) {
    super(coordinator, checkThroughInstanceList, checkInstance, segId, ioMethodCallback,
        membershipWhenIoCome);
    this.caughtIncompleteGroupException = false;

    this.className = "CheckSecondaryReachableCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    logger.info("complete, ori:{} check secondary:{} reachable at:{}", requestId, checkInstance,
        segId);
    LoggerTracer.getInstance()
        .mark(requestId, className, "complete, ori:{} check secondary:{} reachable at:{}",
            requestId,
            checkInstance, segId);
    super.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e, InstanceId passByMeToCheck) {
    logger
        .warn("failed, ori:{} check secondary:{} reachable at:{}", requestId, checkInstance, segId,
            e);
    LoggerTracer.getInstance()
        .mark(requestId, className, "failed, ori:{} check secondary:{} reachable at:{}", requestId,
            checkInstance, segId, e);
    if (e instanceof DisconnectionException || e instanceof SegmentNotFoundException
        || e instanceof NetworkUnhealthyException || e instanceof SegmentDeleteException
        || e instanceof ServerPausedException || e instanceof ServerShutdownException) {
      triggerByCheckCallback.markRequestFailed(passByMeToCheck);
    }
    if (e instanceof IncompleteGroupException) {
      triggerByCheckCallback.markDoneDirectly();
      caughtIncompleteGroupException = true;
    }
    super.fail(e, passByMeToCheck);
  }

  @Override
  public void nextStepProcess() {
    if (this.responseMembership != null) {
      logger.info("ori:{} got membership:{} to update at:{}", requestId, this.responseMembership,
          segId);
      LoggerTracer.getInstance()
          .mark(requestId, className, "ori:{} got membership:{} to update at:{}", requestId,
              this.responseMembership, segId);
      coordinator.updateMembershipFromDatanode(triggerByCheckCallback.getVolumeId(), segId,
          responseMembership,
          triggerByCheckCallback);
    }
    if (secondaryUnreachable()) {
      logger.info("ori:{} check secondary:{} at:{} in membership:{} is unreachable !!!", requestId,
          checkInstance,
          segId, membershipWhenIoCome);
      logger.info("ori:{} now mark secondary:{} at:{} in membership:{} down status", requestId,
          checkInstance,
          segId, membershipWhenIoCome);
      LoggerTracer.getInstance()
          .mark(requestId, className, "ori:{} mark secondary {} down:{} at:{}", requestId,
              checkInstance,
              this.responseMembership, segId);
      coordinator.markSecondaryDown(membershipWhenIoCome, checkInstance.getInstanceId());
    } else {
      logger
          .info("ori:{} check secondary:{} at:{} in membership:{} is still reachable...", requestId,
              checkInstance, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} check secondary:{} at:{} in membership:{} is still reachable...", requestId,
          checkInstance,
          segId, membershipWhenIoCome);
    }
    if (caughtIncompleteGroupException) {

      logger.info(
          "ori:{} check secondary caught an IncompleteGroupException, should check primary status"
              + " at:{} in:{}",
          requestId, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} check secondary caught an IncompleteGroupException, should check primary  "
              + "status at:{} in:{}",
          requestId, segId, membershipWhenIoCome);
      coordinator
          .checkPrimaryReachable(this.membershipWhenIoCome, membershipWhenIoCome.getPrimary(),
              false,
              triggerByCheckCallback, new IncompleteGroupException(), segId);
    } else {
      done();
    }
  }
}
