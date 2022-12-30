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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.coordinator.lib.Coordinator;
import py.instance.InstanceId;
import py.instance.SimpleInstance;
import py.membership.SegmentMembership;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.exception.MembershipVersionLowerException;
import py.netty.exception.NotPrimaryException;
import py.netty.exception.NotSecondaryException;
import py.proto.Broadcastlog;
import py.volume.VolumeType;


public class CheckRequestCallbackCollector {

  private static final Logger logger = LoggerFactory.getLogger(CheckRequestCallbackCollector.class);
  private final VolumeType volumeType;
  public SegmentMembership responseMembership;
  public Coordinator coordinator;
  public TriggerByCheckCallback triggerByCheckCallback;
  public List<SimpleInstance> checkThroughInstanceList;
  public SegmentMembership membershipWhenIoCome;
  public SimpleInstance checkInstance;
  protected AtomicInteger goodCount;
  protected AtomicInteger goodCountOfSecondary;
  protected long requestId;
  protected String className;
  protected SegId segId;

  boolean membershipLower = false;
  private AtomicInteger leftCount;
  private int sendCount;
  private AtomicInteger reachableCount;
  private AtomicInteger unreachableCount;
  private AtomicInteger unreachableCountOfSecondary;
  private Map<Long, Long> recordInstanceIdAndPclId;
  private Set<Long> recordInstanceIdAndMigrating;
  private long selectedTempPrimary = -1;



  public CheckRequestCallbackCollector(Coordinator coordinator,
      List<SimpleInstance> checkThroughInstanceList,
      SimpleInstance checkInstance, SegId segId, TriggerByCheckCallback triggerByCheckCallback,
      SegmentMembership membershipWhenIoCome) {
    Validate.notNull(checkThroughInstanceList);
    this.volumeType = coordinator.getVolumeType(triggerByCheckCallback.getVolumeId());
    this.leftCount = new AtomicInteger(checkThroughInstanceList.size());
    this.sendCount = checkThroughInstanceList.size();

    Validate.isTrue(this.sendCount < volumeType.getNumMembers());
    this.reachableCount = new AtomicInteger(0);
    this.unreachableCount = new AtomicInteger(0);
    this.unreachableCountOfSecondary = new AtomicInteger(0);
    this.goodCount = new AtomicInteger(0);
    this.goodCountOfSecondary = new AtomicInteger(0);
    this.checkInstance = checkInstance;
    this.segId = segId;
    this.recordInstanceIdAndPclId = new ConcurrentHashMap<>();
    this.recordInstanceIdAndMigrating = new HashSet<>();
    this.coordinator = coordinator;
    this.triggerByCheckCallback = triggerByCheckCallback;
    this.requestId = triggerByCheckCallback.getOriRequestId();
    this.checkThroughInstanceList = checkThroughInstanceList;
    this.membershipWhenIoCome = membershipWhenIoCome;
    if (this.leftCount.get() == 0) {
      logger.error("no member can do check job");
      done();
    }
  }



  public void complete(Broadcastlog.PbCheckResponse object, InstanceId passByMeToCheck) {
    try {
      this.goodCount.incrementAndGet();
      if (object.hasPbMembership()) {
        SegmentMembership membership = PbRequestResponseHelper
            .buildMembershipFrom(object.getPbMembership());
        updateMembership(membership);
      }

      boolean isSecondary = membershipWhenIoCome.getSecondaries().contains(passByMeToCheck);
      if (isSecondary) {
        goodCountOfSecondary.incrementAndGet();
      }

      if (object.hasMigrating() && object.getMigrating()) {
        recordInstanceIdAndMigrating.add(object.getMyInstanceId());
      }

      if (!object.getReachable()) {
        this.unreachableCount.incrementAndGet();
        if (isSecondary) {
          unreachableCountOfSecondary.incrementAndGet();
        }
        if (object.hasPcl()) {
          Validate.isTrue(object.hasMyInstanceId());
          Validate.isTrue(object.getMyInstanceId() > 0);
          recordInstanceIdAndPclId.put(object.getMyInstanceId(), object.getPcl());
        }
      } else {
        this.reachableCount.incrementAndGet();
      }

    } catch (Exception e) {
      logger.error("get reachable result failed", e);
    } finally {
      tryNextStepProcess();
    }

  }



  public void fail(Exception e, InstanceId passByMeToCheck) {
    SegmentMembership membership = null;
    boolean needMarkUpdateMembership = false;
    try {
      if (e instanceof MembershipVersionLowerException) {

        membership = NettyExceptionHelper
            .getMembershipFromBuffer(((MembershipVersionLowerException) e).getMembership());
        membershipLower = true;
      } else if (e instanceof NotSecondaryException) {
        membership = NettyExceptionHelper
            .getMembershipFromBuffer(((NotSecondaryException) e).getMembership());
      } else if (e instanceof NotPrimaryException) {
        membership = NettyExceptionHelper
            .getMembershipFromBuffer(((NotPrimaryException) e).getMembership());
      } else {

        needMarkUpdateMembership = true;
      }

    } catch (Exception e1) {
      needMarkUpdateMembership = true;
      logger.error("ori:{} failed to get membership", triggerByCheckCallback.getOriRequestId(), e);
    }
    if (needMarkUpdateMembership) {
      this.triggerByCheckCallback.markNeedUpdateMembership();
    }

    updateMembership(membership);
    tryNextStepProcess();
  }

  private void tryNextStepProcess() {
    if (this.leftCount.decrementAndGet() == 0) {
      nextStepProcess();
    }
  }


  public boolean confirmPrimaryUnreachable() {
    return quorumCompleted() && allSecondaryCompleted();
  }

  public boolean confirmTempPrimaryUnreachable() {
    return quorumCompleted();
  }


  public boolean primaryUnreachable() {
    return !membershipLower && allMembersCompleteUnreachable() && quorumCompleted();
  }

  public boolean tempPrimaryUnreachable() {
    return allMembersCompleteUnreachable() && quorumCompleted();
  }


  private boolean quorumCompleted() {
    return this.goodCount.get() >= volumeType.getVotingQuorumSize();
  }


  private boolean allSecondaryCompleted() {
    boolean allSecondaryCompleted =
        goodCount.get() == sendCount || goodCountOfSecondary.get() == membershipWhenIoCome
            .getSecondaries().size();
    if (allSecondaryCompleted) {
      return true;
    } else {
      logger
          .warn("ori:{} sendcount {} goodCount {} good secondary {} is less", requestId, sendCount,
              goodCount.get(), goodCountOfSecondary);
      return false;
    }
  }


  @Deprecated
  private boolean allSecondaryUnreachable() {
    int aliveSecondaryCount = Math
        .min(goodCountOfSecondary.get(), membershipWhenIoCome.getSecondaries().size());
    boolean allSecondarySayUnreachable = unreachableCountOfSecondary.get() == aliveSecondaryCount;
    if (allSecondarySayUnreachable) {
      return true;
    } else {
      logger.warn(
          "ori:{} unreachable {} unreachable secondary {} is less than good s {} ?, record {} ",
          requestId, unreachableCount.get(), unreachableCountOfSecondary, goodCountOfSecondary,
          recordInstanceIdAndPclId);
      return false;
    }
  }


  private boolean allMembersCompleteUnreachable() {
    boolean allAliveMembersSayUnreachable = unreachableCount.get() == goodCount.get();
    if (allAliveMembersSayUnreachable) {
      return true;
    } else {
      logger.warn(
          "ori:{} sendCount {}  goodCount {} unreachable {}, some one complete true, pcl record "
              + "{} ",
          requestId, sendCount, goodCount.get(), unreachableCount.get(), recordInstanceIdAndPclId);
      return false;
    }
  }



  public boolean primaryReachable() {
    return this.reachableCount.get() > 0;
  }

  public boolean tempPrimaryReachable() {
    return this.reachableCount.get() > 0;
  }


  public boolean secondaryUnreachable() {
    return this.unreachableCount.get() == sendCount;
  }



  public Long getTempPrimaryId() {
    selectedTempPrimary = getTempPrimaryIdByPcl();
    logger.warn(
        "i have selected a temp primary {} from record {}, migrating {}, for seg {} type {}, "
            + "response membership {}",
        selectedTempPrimary, recordInstanceIdAndPclId, recordInstanceIdAndMigrating, segId,
        volumeType, responseMembership);
    return selectedTempPrimary;
  }


  public Long getTempPrimaryIdByPcl() {

    boolean selectedTpIsMigrating = false;
    Long tempPrimaryId = -1L;
    Long maxPclId = -1L;
    for (Map.Entry<Long, Long> entry : recordInstanceIdAndPclId.entrySet()) {
      long instanceId = entry.getKey();
      long pclId = entry.getValue();
      if (pclId == -1) {
        continue;
      }
      if (pclId > maxPclId) {

        maxPclId = pclId;
        tempPrimaryId = instanceId;
        selectedTpIsMigrating = recordInstanceIdAndMigrating.contains(tempPrimaryId);
      } else if (pclId == maxPclId) {

        boolean iamMigrating = recordInstanceIdAndMigrating.contains(instanceId);
        if (iamMigrating) {
          continue;
        }


        boolean icanReplaceYou = selectedTpIsMigrating || instanceId > tempPrimaryId;
        if (icanReplaceYou) {
          tempPrimaryId = instanceId;
          maxPclId = pclId;
          selectedTpIsMigrating = iamMigrating;
        }
      }
    }

    if (tempPrimaryId != -1 && selectedTpIsMigrating) {
      logger.error(
          "can not select a temp primary from record {}, the one {} has max pcl is migrating ",
          recordInstanceIdAndPclId, tempPrimaryId);
      tempPrimaryId = -1L;
    }

    return tempPrimaryId;
  }


  @Deprecated
  public Long getTempPrimaryIdByInstanceId() {
    Long tempPrimaryId = -1L;
    Long bigInstanceId = Long.MIN_VALUE;
    Long smallInstanceId = Long.MAX_VALUE;


    for (Map.Entry<Long, Long> entry : recordInstanceIdAndPclId.entrySet()) {
      if (entry.getValue() == -1) {
        continue;
      }
      long instanceId = entry.getKey();

      if (instanceId > bigInstanceId) {
        bigInstanceId = instanceId;
      }
      if (instanceId <= smallInstanceId) {
        smallInstanceId = instanceId;
      }
    }

    Validate.isTrue(bigInstanceId < Long.MAX_VALUE && smallInstanceId > Long.MIN_VALUE);
    if (segId.getIndex() % 2 == 0) {
      tempPrimaryId = bigInstanceId;
      logger.warn("select instance {} who's id bigger for seg {}", bigInstanceId, segId);
    } else {
      tempPrimaryId = smallInstanceId;
      logger.warn("select instance {} who's id little for seg {}", bigInstanceId, segId);
    }
    return tempPrimaryId;
  }

  public void nextStepProcess() {
    throw new NotImplementedException("not implement here");
  }

  public void done() {
    triggerByCheckCallback.triggeredByCheckCallback();
  }



  public void updateMembership(SegmentMembership membership) {
    if (membership == null) {
      return;
    }
    if (this.responseMembership == null) {
      this.responseMembership = membership;
    } else if (this.responseMembership.compareTo(membership) < 0) {
      logger.info("update membership:{} instead of:{}", membership, responseMembership);
      this.responseMembership = membership;
    }
  }

  public int getGoodCount() {
    return goodCount.get();
  }

  public int getReachableCount() {
    return reachableCount.get();
  }

  public int getUnReachableCount() {
    return unreachableCount.get();
  }

  public int getLeftCount() {
    return leftCount.get();
  }

  public SegmentMembership getResponseMembership() {
    return responseMembership;
  }
}


