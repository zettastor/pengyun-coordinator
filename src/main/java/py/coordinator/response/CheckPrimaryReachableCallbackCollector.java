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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.common.RequestIdBuilder;
import py.coordinator.lib.Coordinator;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.netty.exception.DisconnectionException;
import py.netty.exception.NetworkUnhealthyException;
import py.netty.exception.NotPrimaryException;
import py.netty.exception.SegmentDeleteException;
import py.netty.exception.SegmentNotFoundException;
import py.netty.exception.ServerPausedException;
import py.netty.exception.ServerShutdownException;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.RequestOption;


public class CheckPrimaryReachableCallbackCollector extends CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(CheckPrimaryReachableCallbackCollector.class);

  private final Exception checkPrimaryCause;
 
 
 
  private InstanceId someOneFailed = null;


  
  public CheckPrimaryReachableCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList, SimpleInstance checkInstanceId, SegId segId,
      TriggerByCheckCallback ioMethodCallback, SegmentMembership membershipWhenIoCome,
      Exception checkPrimaryCause) {
    super(coordinator, checkThroughInstanceList, checkInstanceId, segId, ioMethodCallback,
        membershipWhenIoCome);
    this.checkPrimaryCause = checkPrimaryCause;
   
    this.className = "CheckPrimaryReachableCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    logger.info("complete, ori:{} secondary:{} check primary:{} reachable {} at:{}", requestId,
        passByMeToCheck,
        checkInstance, object.getReachable(), segId);
    LoggerTracer.getInstance()
        .mark(requestId, className,
            "complete, ori:{} secondary:{} check primary:{} at:{}, result={}",
            requestId, passByMeToCheck, checkInstance, segId, object.getReachable());
    super.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e, InstanceId passByMeToCheck) {
    if (e instanceof DisconnectionException || e instanceof SegmentNotFoundException
        || e instanceof NetworkUnhealthyException || e instanceof SegmentDeleteException
        || e instanceof ServerPausedException || e instanceof ServerShutdownException) {
      triggerByCheckCallback.markRequestFailed(passByMeToCheck);
    }

    logger.warn("failed, ori:{} secondary:{} check primary:{} reachable at:{}", requestId,
        passByMeToCheck,
        checkInstance, segId, e);
    LoggerTracer.getInstance()
        .mark(requestId, className, "failed, ori:{} secondary {} check primary:{} reachable at:{}",
            requestId,
            passByMeToCheck, checkInstance, segId, e);
    someOneFailed = passByMeToCheck;
    super.fail(e, passByMeToCheck);
  }

  @Override
  public void nextStepProcess() {
    if (this.responseMembership != null) {
      coordinator.updateMembershipFromDatanode(triggerByCheckCallback.getVolumeId(), segId,
          responseMembership, triggerByCheckCallback);
    }

    if (primaryUnreachable()) {
     
     
     
      logger.warn("ori:{} primary:{} at:{} in membership:{} is unreachable !!!",
          requestId, checkInstance, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
          "ori:{} primary:{} at:{} in membership:{} is unreachable !!!",
          requestId, checkInstance, segId, membershipWhenIoCome);

      goingToMarkTmpPrimary();
    } else {
      SegmentForm segmentForm = SegmentForm.getSegmentForm(membershipWhenIoCome,
          coordinator.getVolumeType(triggerByCheckCallback.getVolumeId()));
      if (primaryReachable() || segmentForm.onlyPrimary()) {
        logger.info(
            "ori:{} primary:{} at:{} in membership:{} is still reachable:{}, segment form:{} mark"
                + " primary as UnstablePrimary",
            requestId, checkInstance, segId, membershipWhenIoCome,
            primaryReachable(), segmentForm);
        LoggerTracer.getInstance().mark(requestId, className,
            "ori:{} primary:{} at:{} in membership:{} is still reachable:{}, segment form:{} mark"
                + " primary as UnstablePrimary",
            requestId, segId, membershipWhenIoCome,
            primaryReachable(), segmentForm);
        
        coordinator
            .markPrimaryAsUnstablePrimary(membershipWhenIoCome, checkInstance.getInstanceId());
      } else {
       
       
        if (triggerByCheckCallback.streamIO()) {
          if (checkPrimaryCause instanceof NotPrimaryException) {
            logger.info(
                "ori:{} primary:{} at:{} in membership:{} not primary and no one can reach it, "
                    + "mark it as unstable",
                requestId, checkInstance, segId, membershipWhenIoCome);
            LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
                "ori:{} primary:{} at:{} in membership:{} not primary and no one can reach it, "
                    + "mark it as unstable",
                requestId, checkInstance, segId, membershipWhenIoCome);
            coordinator
                .markPrimaryAsUnstablePrimary(membershipWhenIoCome, checkInstance.getInstanceId());
          } else {
            logger.info(
                "ori:{} primary:{} at:{} in membership:{} consider as unreachable!!! mark it down.",
                checkInstance, segId, membershipWhenIoCome);
            LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
                "ori:{} primary:{} at:{} in membership:{} consider as unreachable!!! mark it down.",
                checkInstance, segId, membershipWhenIoCome);
            triggerByCheckCallback.markRequestFailed(checkInstance.getInstanceId());
            coordinator.markPrimaryDown(membershipWhenIoCome, checkInstance.getInstanceId());
          }
        } else {
          logger.info("ori:{} primary:{} at:{} in membership:{} is still reachable...",
              checkInstance, segId, membershipWhenIoCome);
          LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
              "ori:{} primary:{} at:{} in membership:{} is still reachable...",
              checkInstance, segId, membershipWhenIoCome);
        }
      }
      done();
    }
  }

  private void goingToMarkTmpPrimary() {
    long tempPrimaryId = getTempPrimaryId();
    if (tempPrimaryId > 0) {
      confirmPrimaryUnreachable(tempPrimaryId);
    } else {
      done();
    }
  }

  private void confirmPrimaryUnreachable(long tempPrimaryId) {
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(checkInstance.getInstanceId().getId());
    checkRequestBuilder.setRequestOption(RequestOption.CONFIRM_UNREACHABLE);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    checkRequestBuilder.setTempPrimary(tempPrimaryId);

    List<SimpleInstance> confirmThroughInstanceList = new ArrayList<>(
        checkThroughInstanceList.size());
    confirmThroughInstanceList.addAll(checkThroughInstanceList);
    SimpleInstance failedOne = null;
    if (someOneFailed != null) {
      for (SimpleInstance simpleInstance : checkThroughInstanceList) {
        if (simpleInstance.getInstanceId() == someOneFailed) {
          failedOne = simpleInstance;
          logger.warn("ori {} a member not failed in check process, no need confrim him {} {}",
              requestId, failedOne, segId);
          break;
        }
      }
      confirmThroughInstanceList.remove(failedOne);
    }

    ConfirmPrimaryUnreachableCallbackCollector confirmUnreachableCollector =
        new ConfirmPrimaryUnreachableCallbackCollector(
            coordinator, confirmThroughInstanceList, checkInstance, segId, triggerByCheckCallback,
            membershipWhenIoCome, tempPrimaryId);

    logger.warn(
        "ori:{} get temp primary:{} send confirm unreachable to zombie secondaries:{} at:{} in "
            + "membership:{}",
        requestId, tempPrimaryId, confirmThroughInstanceList, segId,
        membershipWhenIoCome);
    LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
        "ori:{} get temp primary:{} send confirm unreachable to zombie secondaries:{} at:{} in "
            + "membership:{}",
        requestId, tempPrimaryId, confirmThroughInstanceList, segId,
        membershipWhenIoCome);
    Broadcastlog.PbCheckRequest confirmUnreachableRequest = checkRequestBuilder.build();
    for (SimpleInstance simpleInstance : confirmThroughInstanceList) {
      CheckRequestCallback cnfirmPrimaryUnReachableCallback = new CheckRequestCallback(
          confirmUnreachableCollector, simpleInstance.getInstanceId());
      coordinator.getClientFactory().generate(simpleInstance.getEndPoint())
          .check(confirmUnreachableRequest, cnfirmPrimaryUnReachableCallback);
    }
  }
}
