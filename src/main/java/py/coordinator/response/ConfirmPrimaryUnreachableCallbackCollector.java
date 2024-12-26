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
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.coordinator.lib.Coordinator;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;


public class ConfirmPrimaryUnreachableCallbackCollector extends CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory
      .getLogger(ConfirmPrimaryUnreachableCallbackCollector.class);

  private InstanceId tempPrimary;



  public ConfirmPrimaryUnreachableCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList, SimpleInstance checkInstance, SegId segId,
      TriggerByCheckCallback ioMethodCallback, SegmentMembership membershipWhenIoCome,
      Long tempPrimaryId) {
    super(coordinator, checkThroughInstanceList, checkInstance, segId, ioMethodCallback,
        membershipWhenIoCome);
    this.tempPrimary = new InstanceId(tempPrimaryId);

    this.className = "ConfirmPrimaryUnreachableCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    logger.info("complete, ori:{} confirm zombie secondary:{} for primary:{} at:{}", requestId,
        passByMeToCheck,
        checkInstance, segId);
    LoggerTracer.getInstance()
        .mark(requestId, className,
            "complete, ori:{} confirm zombie secondary:{} for primary:{} at:{}",
            requestId, passByMeToCheck, checkInstance, segId);
    if (object.hasPbMembership()) {
      SegmentMembership membership = PbRequestResponseHelper
          .buildMembershipFrom(object.getPbMembership());
      logger.warn(
          "got new membership:{}, should update it! ori:{} confirm zombie secondary {} at:{} ",
          membership, requestId, passByMeToCheck, segId);
      updateMembership(membership);
    }

    super.complete(object, passByMeToCheck);
  }

  @Override
  public void fail(Exception e, InstanceId passByMeToCheck) {
    logger.warn("failed, ori:{} confirm zombie secondary:{} for primary:{} at:{}", requestId,
        passByMeToCheck,
        checkInstance, segId, e);
    LoggerTracer.getInstance()
        .mark(requestId, className,
            "failed, ori:{} confirm zombie secondary:{} for primary:{} at:{}",
            requestId, passByMeToCheck, checkInstance, segId, e);
    super.fail(e, passByMeToCheck);
  }

  @Override
  public void nextStepProcess() {

    if (confirmPrimaryUnreachable()) {
      logger.warn("ori:{} quorum secondaries:{} for primary:{} at:{} in membership:{} confirmed",
          requestId,
          goodCount, checkInstance, segId, membershipWhenIoCome);
      logger.warn("ori:{} now mark primary {} at:{} in membership:{} down status", requestId,
          checkInstance,
          segId, membershipWhenIoCome);
      LoggerTracer.getInstance()
          .mark(requestId, className,
              "ori:{} now mark primary {} at:{} in membership:{} down status",
              requestId, checkInstance, segId, membershipWhenIoCome);


      triggerByCheckCallback.replaceLogUuidForNotCreateCompletelyLogs();

      coordinator.markPrimaryDown(membershipWhenIoCome, checkInstance.getInstanceId());

      if (this.responseMembership != null) {
        if (this.membershipWhenIoCome.compareTo(this.responseMembership) < 0) {
          if (this.responseMembership.getTempPrimary() != null) {
            MemberIoStatus memberIoStatus = this.responseMembership
                .getMemberIoStatus(checkInstance.getInstanceId());
            if (memberIoStatus != MemberIoStatus.Primary) {
              Validate.isTrue(false,
                  "for now primary:" + checkInstance + " should be primary at:"
                      + this.responseMembership);
            }
            logger.info(
                "ori:{} quorum secondaries:{} confirm primary:{} at:{} is down, and return new "
                    + "membership:{} for IO, mark primary down in new membership immediately",
                requestId, goodCount, checkInstance, segId, this.responseMembership);
            LoggerTracer.getInstance().mark(requestId, className,
                "ori:{} quorum secondaries:{} confirm primary:{} at:{} is down, and return new "
                    + "membership:{} for IO, mark primary down in new membership immediately",
                requestId, goodCount, segId, checkInstance, this.responseMembership);

            coordinator.markPrimaryDown(this.responseMembership, checkInstance.getInstanceId());
          }
        }
      }

      if (!membershipWhenIoCome.isSecondary(tempPrimary)) {
        Validate.isTrue(false,
            "ori:" + triggerByCheckCallback.getOriRequestId() + " temp primary:" + tempPrimary
                + " must be secondary at:" + membershipWhenIoCome);
      }

      if (coordinator.markSecondaryAsTempPrimary(membershipWhenIoCome, tempPrimary)) {
        logger.info("ori:{} now mark secondary {} at:{} in membership:{} TempPrimary status",
            requestId,
            tempPrimary, segId, membershipWhenIoCome);
        LoggerTracer.getInstance().mark(requestId, className,
            "ori:{} now mark secondary {} at:{} in membership:{} TempPrimary status", requestId,
            tempPrimary, segId, membershipWhenIoCome);
      }
    } else {
      logger.info(
          "ori:{} not enough secondaries:{} for primary:{} at:{} in membership:{} confirmed, can "
              + "not mark primary down",
          requestId, goodCount, checkInstance, segId, membershipWhenIoCome);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} not enough secondaries:{} for primary:{} at:{} in membership:{} confirmed, can "
              + "not mark primary down",
          requestId, goodCount, checkInstance, segId, membershipWhenIoCome);
    }

    if (responseMembership != null) {
      coordinator.updateMembershipFromDatanode(triggerByCheckCallback.getVolumeId(), segId,
          responseMembership,
          triggerByCheckCallback);
    }

    done();
  }

}
