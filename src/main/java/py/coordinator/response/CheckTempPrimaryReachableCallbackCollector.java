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


public class CheckTempPrimaryReachableCallbackCollector extends CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(CheckTempPrimaryReachableCallbackCollector.class);

  private final Exception checkPrimaryCause;


  
  public CheckTempPrimaryReachableCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList, SimpleInstance checkInstanceId, SegId segId,
      TriggerByCheckCallback ioMethodCallback, SegmentMembership membershipWhenIoCome,
      Exception checkPrimaryCause) {
    super(coordinator, checkThroughInstanceList, checkInstanceId, segId, ioMethodCallback,
        membershipWhenIoCome);
    this.checkPrimaryCause = checkPrimaryCause;
   
    this.className = "CheckTempPrimaryReachableCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    logger.warn("complete, ori:{} secondary {} check temp primary:{} reachable {} at:{}", requestId,
        passByMeToCheck, checkInstance, object.getReachable(), segId);
    LoggerTracer.getInstance().mark(requestId, className,
        "complete, ori:{} secondary {}  check temp primary:{} reachable at:{}, result={}",
        requestId,
        passByMeToCheck, checkInstance, segId, object.getReachable());
    super.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e, InstanceId passByMeToCheck) {
    if (e instanceof DisconnectionException || e instanceof SegmentNotFoundException
        || e instanceof NetworkUnhealthyException || e instanceof SegmentDeleteException
        || e instanceof ServerPausedException || e instanceof ServerShutdownException) {
      triggerByCheckCallback.markRequestFailed(passByMeToCheck);
    }

    logger.warn("failed, ori:{} secondary {} check temp primary:{} reachable at:{}", requestId,
        passByMeToCheck,
        checkInstance, segId, e);
    LoggerTracer.getInstance()
        .mark(requestId, className,
            "failed, ori:{} secondary {} check temp primary:{} reachable at:{}",
            requestId, passByMeToCheck, checkInstance, segId, e);
    super.fail(e, passByMeToCheck);
  }

  @Override
  public void nextStepProcess() {
    if (this.responseMembership != null) {
      coordinator.updateMembershipFromDatanode(triggerByCheckCallback.getVolumeId(), segId,
          this.responseMembership,
          this.triggerByCheckCallback);
    }

    
    if (tempPrimaryUnreachable()) {
     
     
     
      logger.warn("ori:{} tp primary:{} at:{} in membership:{} is unreachable !!!", requestId,
          checkInstance,
          segId, membershipWhenIoCome);
      LoggerTracer.getInstance()
          .mark(requestId, className,
              "ori:{} tp primary:{} at:{} in membership:{} is unreachable !!!",
              requestId, checkInstance, segId, membershipWhenIoCome);

      if (this.responseMembership != null) {
        coordinator.markPrimaryDown(responseMembership, responseMembership.getPrimary());
      }
      goingToMarkTmpPrimary();
    } else {
      SegmentForm segmentForm = SegmentForm.getSegmentForm(membershipWhenIoCome,
          coordinator.getVolumeType(triggerByCheckCallback.getVolumeId()));
      if (tempPrimaryReachable()) {
        logger.warn(
            "ori:{} tp primary:{} at:{} in membership:{} is still reachable:{}, segment form:{} "
                + "mark primary as UnstablePrimary",
            requestId, checkInstance, segId, membershipWhenIoCome, primaryReachable(), segmentForm);
        LoggerTracer.getInstance().mark(requestId, className,
            "ori:{} tp primary:{} at:{} in membership:{} is still reachable:{}, segment form:{} "
                + "mark primary as UnstablePrimary",
            requestId, checkInstance, segId, membershipWhenIoCome, primaryReachable(), segmentForm);
      } else {
       
       
       
        if (triggerByCheckCallback.streamIO()) {
          if (checkPrimaryCause instanceof NotPrimaryException) {
            logger.info(
                "ori:{} tp primary:{} at:{} in membership:{} not primary and no one can reach it,"
                    + " mark it as unstable",
                requestId, checkInstance, segId, membershipWhenIoCome);
            LoggerTracer.getInstance().mark(requestId, className,
                "ori:{} tp primary:{} at:{} in membership:{} not primary and no one can reach it,"
                    + " mark it as unstable",
                requestId, checkInstance, segId, membershipWhenIoCome);
            coordinator
                .markPrimaryAsUnstablePrimary(membershipWhenIoCome, checkInstance.getInstanceId());
          } else {
            logger.info(
                "ori:{} tp primary:{} at:{} in membership:{} consider as unreachable!!! mark it "
                    + "down.",
                requestId, checkInstance, segId, membershipWhenIoCome);
            LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
                "ori:{} tp primary:{} at:{} in membership:{} consider as unreachable!!! mark it "
                    + "down.",
                requestId, checkInstance, segId, membershipWhenIoCome);
            triggerByCheckCallback.markRequestFailed(checkInstance.getInstanceId());
            coordinator.markTempPrimaryDown(membershipWhenIoCome, checkInstance.getInstanceId());
          }
        } else {
          logger
              .info("ori:{} tp primary:{} at:{} in membership:{} is still reachable...", requestId,
                  checkInstance, segId, membershipWhenIoCome);
          LoggerTracer.getInstance().mark(triggerByCheckCallback.getOriRequestId(), className,
              "ori:{} tp primary:{} at:{} in membership:{} is still reachable...", requestId,
              checkInstance, segId, membershipWhenIoCome);
        }
      }
      done();
    }
  }


  
  public void goingToMarkTmpPrimary() {
    long tempPrimaryId = getTempPrimaryId();
    if (tempPrimaryId > 0) {
      confirmTempPrimaryUnreachable(tempPrimaryId);
    } else {
      done();
    }
  }

  private void confirmTempPrimaryUnreachable(long tempPrimaryId) {
    Broadcastlog.PbCheckRequest.Builder checkRequestBuilder = Broadcastlog.PbCheckRequest
        .newBuilder();
    checkRequestBuilder.setRequestId(RequestIdBuilder.get());
    checkRequestBuilder.setCheckInstanceId(checkInstance.getInstanceId().getId());
    checkRequestBuilder.setRequestOption(RequestOption.CONFIRM_TP_UNREACHABLE);
    checkRequestBuilder.setRequestPbMembership(
        PbRequestResponseHelper.buildPbMembershipFrom(membershipWhenIoCome));
    checkRequestBuilder.setVolumeId(segId.getVolumeId().getId());
    checkRequestBuilder.setSegIndex(segId.getIndex());
    checkRequestBuilder.setTempPrimary(tempPrimaryId);

    ConfirmTempPrimaryUnreachableCallbackCollector confirmUnreachableCollector =
        new ConfirmTempPrimaryUnreachableCallbackCollector(
            coordinator, checkThroughInstanceList, checkInstance, segId, triggerByCheckCallback,
            membershipWhenIoCome, tempPrimaryId);

    logger.info(
        "ori:{} get temp primary:{} tp send confirm unreachable to zombie secondaries:{} at:{} in"
            + " membership:{}",
        requestId, tempPrimaryId, checkThroughInstanceList, segId, membershipWhenIoCome);
    LoggerTracer.getInstance().mark(requestId, className,
        "ori:{} tp get temp primary:{} send confirm unreachable to zombie secondaries:{} at:{} in"
            + " membership:{}",
        requestId, tempPrimaryId, checkThroughInstanceList, segId, membershipWhenIoCome);
    Broadcastlog.PbCheckRequest confirmUnreachableRequest = checkRequestBuilder.build();
    for (SimpleInstance simpleInstance : checkThroughInstanceList) {
      CheckRequestCallback confirmPrimaryUnReachableCallback = new CheckRequestCallback(
          confirmUnreachableCollector, simpleInstance.getInstanceId());
      coordinator.getClientFactory().generate(simpleInstance.getEndPoint())
          .check(confirmUnreachableRequest, confirmPrimaryUnReachableCallback);
    }
  }
}
