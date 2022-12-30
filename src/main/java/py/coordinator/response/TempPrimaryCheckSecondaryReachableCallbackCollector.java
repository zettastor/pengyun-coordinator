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


public class TempPrimaryCheckSecondaryReachableCallbackCollector extends
    CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(TempPrimaryCheckSecondaryReachableCallbackCollector.class);
  private boolean caughtIncompleteGroupException;



  public TempPrimaryCheckSecondaryReachableCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList, SimpleInstance checkInstance, SegId segId,
      TriggerByCheckCallback ioMethodCallback, SegmentMembership membershipWhenIoCome) {
    super(coordinator, checkThroughInstanceList, checkInstance, segId, ioMethodCallback,
        membershipWhenIoCome);
    this.caughtIncompleteGroupException = false;

    this.className = "TempPrimaryCheckSecondaryReachableCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    logger.info("complete, ori:{} Tp {} check secondary:{} reachable {} at:{}", requestId,
        passByMeToCheck, checkInstance,
        object.getReachable(), segId);
    LoggerTracer.getInstance()
        .mark(requestId, className, "complete, ori:{} Tp check secondary:{} reachable at:{}",
            requestId,
            checkInstance, object.getReachable(), segId);
    super.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e, InstanceId passByMeToCheck) {
    logger.warn("failed, ori:{} Tp check secondary:{} reachable at:{}", requestId, checkInstance,
        segId, e);
    LoggerTracer.getInstance()
        .mark(requestId, className, "failed, ori:{} Tp check secondary:{} reachable at:{}",
            requestId,
            checkInstance, segId, e);
    if (e instanceof DisconnectionException || e instanceof SegmentNotFoundException
        || e instanceof NetworkUnhealthyException || e instanceof SegmentDeleteException
        || e instanceof ServerPausedException || e instanceof ServerShutdownException) {
      triggerByCheckCallback.markRequestFailed(passByMeToCheck);
    }
    if (e instanceof IncompleteGroupException) {
      logger.warn(
          "ori:{} Tp check secondary:{} reachable at:{}, membership:{}, should mark done "
              + "directly, caught an exception",
          requestId, checkInstance, segId, membershipWhenIoCome, e);
      triggerByCheckCallback.markDoneDirectly();
      caughtIncompleteGroupException = true;
    }
    super.fail(e, passByMeToCheck);
  }

  @Override
  public void nextStepProcess() {
    if (this.responseMembership != null) {
      logger.warn("ori:{} Tp got membership:{} to update at:{}", requestId, this.responseMembership,
          segId);
      LoggerTracer.getInstance()
          .mark(requestId, className, "ori:{} Tp got membership:{} to update at:{}", requestId,
              this.responseMembership, segId);
      if (responseMembership.getTempPrimary() != null) {
        logger.info("ori: {} Tp exists, should mark primary down in new membership");
        coordinator.markPrimaryDown(responseMembership, responseMembership.getPrimary());
      }
      coordinator
          .updateMembershipFromDatanode(triggerByCheckCallback.getVolumeId(), segId,
              this.responseMembership,
              this.triggerByCheckCallback);
    }
    if (secondaryUnreachable()) {
      logger
          .warn("ori:{} Tp check secondary:{} at:{} in membership:{} is unreachable !!!", requestId,
              checkInstance, segId, membershipWhenIoCome);
      LoggerTracer.getInstance()
          .mark(requestId, className, "ori:{} Tp mark secondary down:{} at:{}", requestId,
              this.responseMembership, segId);
      coordinator.markSecondaryDown(membershipWhenIoCome, checkInstance.getInstanceId());
    } else {
      logger.warn("ori:{} Tp check secondary:{} at:{} in membership:{} is still reachable...",
          requestId,
          checkInstance, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} Tp check secondary:{} at:{} in membership:{} is still reachable...", requestId,
          checkInstance, segId, membershipWhenIoCome);
    }
    if (caughtIncompleteGroupException) {

      logger.warn(
          "ori:{} Tp check secondary caught an IncompleteGroupException, should check primary "
              + "status at:{} in:{}",
          requestId, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} Tp check secondary caught an IncompleteGroupException, should check primary  "
              + "status at:{} in:{}",
          requestId, segId, membershipWhenIoCome);

      coordinator.checkTempPrimaryReachable(this.membershipWhenIoCome,
          membershipWhenIoCome.getTempPrimary(), false,
          triggerByCheckCallback, new IncompleteGroupException(), segId);
    } else {
      done();
    }
  }
}
