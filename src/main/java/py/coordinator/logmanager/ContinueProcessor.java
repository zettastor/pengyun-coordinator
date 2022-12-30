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

package py.coordinator.logmanager;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.coordinator.lib.Coordinator;
import py.coordinator.response.TriggerByCheckCallback;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentMembership;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.exception.DisconnectionException;
import py.netty.exception.HasNewPrimaryException;
import py.netty.exception.IncompleteGroupException;
import py.netty.exception.MembershipVersionHigerException;
import py.netty.exception.MembershipVersionLowerException;
import py.netty.exception.NetworkUnhealthyBySdException;
import py.netty.exception.NetworkUnhealthyException;
import py.netty.exception.NotPrimaryException;
import py.netty.exception.NotSecondaryException;
import py.netty.exception.SegmentDeleteException;
import py.netty.exception.SegmentNotFoundException;
import py.netty.exception.ServerPausedException;
import py.netty.exception.ServerShutdownException;
import py.netty.exception.SnapshotMismatchException;
import py.volume.VolumeType;


public class ContinueProcessor {

  private static final Logger logger = LoggerFactory.getLogger(ContinueProcessor.class);

  long oriId;
  long requestId;
  Coordinator coordinator;
  long volumeId;
  SegId segId;
  TriggerByCheckCallback callback;
  IoMember ioMember;
  String requestType;
  IoContextManagerForAsyncDatanodeCallback ioContextManager;
  private SegmentMembership membershipWhenIoCome;


  
  public ContinueProcessor(Coordinator coordinator, long volumeId, SegId segId, long oriId,
      long requestId,
      IoMember ioMember, SegmentMembership membership, TriggerByCheckCallback callback,
      IoContextManagerForAsyncDatanodeCallback ioContextManager) {
    this.coordinator = coordinator;
    this.volumeId = volumeId;
    this.segId = segId;
    this.oriId = oriId;
    this.requestId = requestId;
    this.ioMember = ioMember;
    this.membershipWhenIoCome = membership;
    this.callback = callback;
    this.ioContextManager = ioContextManager;
    if (ioContextManager == null) {
      requestType = "Commit";
    } else if (ioContextManager instanceof WriteIoContextManager) {
      requestType = "Write";
    } else {
      requestType = "Read";
    }
  }


  
  public boolean processException(Exception exception, String className)
      throws InvalidProtocolBufferException {
    boolean continueProcess = true;
    MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();

    if (exception instanceof MembershipVersionLowerException) {
      SegmentMembership membership = NettyExceptionHelper
          .getMembershipFromBuffer(((MembershipVersionLowerException) exception).getMembership());
      coordinator.updateMembershipFromDatanode(volumeId, segId,
          membership, callback);
    } else if (exception instanceof NotSecondaryException) {
      SegmentMembership membership = NettyExceptionHelper
          .getMembershipFromBuffer(((NotSecondaryException) exception).getMembership());
      coordinator.updateMembershipFromDatanode(volumeId, segId,
          membership, callback);
    } else if (exception instanceof NotPrimaryException) {
      logger.info("ori:{} {} request:{} met unstable primary:{} at:{}", oriId, requestType,
          requestId, ioMember.getEndPoint(), segId,
          exception);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} {} request:{} met unstable primary:{} at:{}", oriId, requestType,
          requestId, ioMember.getEndPoint(), segId,
          exception);
      if (!memberIoStatus.isPrimary()) {
        Validate.isTrue(false,
            "io member:" + ioMember + ", current membership:" + this.membershipWhenIoCome + ", at:"
                + segId);
      }
     
     
      continueProcess = gotoCheck(exception, memberIoStatus, false);
    } else if (exception instanceof DisconnectionException
        || exception instanceof NetworkUnhealthyException) {
      logger.info("ori:{} {} request:{} can not reach:{} at:{}", oriId, requestType,
          requestId, ioMember.getEndPoint(), segId,
          exception);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} {} request:{} can not reach:{} at:{}", oriId, requestType,
          requestId, ioMember.getEndPoint(), segId,
          exception);

      continueProcess = gotoCheck(exception, memberIoStatus, false);
    } else if (exception instanceof SegmentNotFoundException
        || exception instanceof SegmentDeleteException
        || exception instanceof ServerPausedException
        || exception instanceof ServerShutdownException) {
      logger.info("ori:{} {} request:{} can not reach:{} at:{}, exception is ", oriId,
          requestType, requestId, ioMember.getEndPoint(), segId,
          exception);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} {} request:{} can not reach:{} at:{}, exception is", oriId,
          requestType, requestId, ioMember.getEndPoint(), segId,
          exception);
      continueProcess = gotoCheck(exception, memberIoStatus, true);
    } else if (exception instanceof NetworkUnhealthyBySdException) {
      logger.info("ori:{} {} request:{} can not reach:{} at:{}, exception is ", oriId,
          requestType, requestId, ioMember.getEndPoint(), segId,
          exception);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} {} request:{} can not reach:{} at:{}, exception is", oriId,
          requestType, requestId, ioMember.getEndPoint(), segId,
          exception);
      if (ioMember.getMemberIoStatus().match(MemberIoStatus.Primary)) {
        continueProcess = gotoCheck(exception, memberIoStatus, true);
      } else {
        continueProcess = gotoCheck(exception, memberIoStatus, true);
       
       
      }
    } else if (exception instanceof IncompleteGroupException) {
      logger.info(
          "ori:{} {} request at:{} get IncompleteGroupException, should mark this io done "
              + "directly, and go check process",
          oriId, requestType, segId);
      LoggerTracer.getInstance().mark(requestId, className,
          "ori:{} {} request:{} can not reach:{} at:{}, exception is", oriId,
          requestType, requestId, ioMember.getEndPoint(), segId,
          exception);
      if (ioContextManager != null) {
        ioContextManager.markDoneDirectly();
      }

      if (!memberIoStatus.isPrimary() && !memberIoStatus.isTempPrimary()) {
        Validate.isTrue(false,
            "IncompleteGroupException should throw by primary, current io instance:" + ioMember
                .getInstanceId() + ", current io status:" + memberIoStatus + ", in membership:"
                + this.membershipWhenIoCome + ", at segId" + segId);
      }
     
      continueProcess = gotoCheck(exception, memberIoStatus, false);
    } else if (exception instanceof HasNewPrimaryException) {
      logger.info(
          "ori:{} {} request at:{} get HasNewPrimaryException, should mark this io done directly,"
              + " and go check process",
          oriId, requestType, segId);
      if (!memberIoStatus.isPrimary() && !memberIoStatus.isTempPrimary()) {
        Validate.isTrue(false,
            "HasNewPrimaryException should throw by primary, current io instance:" + ioMember
                .getInstanceId() + ", current io status:" + memberIoStatus + ", in membership:"
                + this.membershipWhenIoCome + ", at segId" + segId);
      }
     
      continueProcess = gotoCheck(exception, memberIoStatus, true);
    } else if (exception instanceof MembershipVersionHigerException) {
     
      continueProcess = true;
    } else {
      logger.warn("ori:{} {} request:{} not process exception for now, just mark it {} {}", oriId,
          requestType, requestId, ioMember, exception);
     
      if (ioContextManager != null) {
        ioContextManager.markDelay();
      }
    }

    return continueProcess;
  }

  private boolean gotoCheck(Exception exception, MemberIoStatus memberIoStatus,
      boolean deletedCase) {
    boolean continueProcess = false;
    if (isLargeVolume()) {
      if (memberIoStatus.isPrimary()) {
        coordinator
            .checkPrimaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(), deletedCase,
                callback, exception, segId);
      } else if (memberIoStatus.isTempPrimary()) {
        coordinator.checkTempPrimaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(),
            deletedCase,
            callback, exception, segId);
      } else {
        if (primaryDown()) {
          if (membershipWhenIoCome.getTempPrimary() != null) {
            coordinator
                .tpCheckSecondaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(),
                    deletedCase,
                    callback, segId);
          } else {
            logger.warn("ori:{} after mark primary down, a secondary {} io fail before tp appear",
                oriId, ioMember.getInstanceId());
            continueProcess = true;
          }
        } else {
          coordinator.checkSecondaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(),
              deletedCase,
              callback, segId);
        }
      }
    } else {
      if (memberIoStatus.isPrimary()) {
        coordinator
            .checkPrimaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(), deletedCase,
                callback, exception, segId);
      } else if (memberIoStatus.isTempPrimary()) {
        logger.error("ori:{} why temp {} io fail!", oriId, ioMember.getInstanceId());
        continueProcess = true;
      } else {
        coordinator.checkSecondaryReachable(this.membershipWhenIoCome, ioMember.getInstanceId(),
            deletedCase,
            callback, segId);
      }
    }
    return continueProcess;
  }

  private boolean isLargeVolume() {
    return coordinator.getVolumeType(volumeId) == VolumeType.LARGE;
  }

  private boolean primaryDown() {
    return this.membershipWhenIoCome.getMemberIoStatus(this.membershipWhenIoCome.getPrimary())
        .isDown();
  }
}
