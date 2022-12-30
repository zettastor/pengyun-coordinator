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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.LoggerTracer;
import py.coordinator.lib.Coordinator;
import py.coordinator.task.GetMembershipTask;
import py.membership.SegmentMembership;
import py.netty.core.AbstractMethodCallback;
import py.proto.Broadcastlog;


public class GetMembershipCallbackCollector extends
    AbstractMethodCallback<Broadcastlog.PbGetMembershipResponse> {

  private static final Logger logger = LoggerFactory
      .getLogger(GetMembershipCallbackCollector.class);
  protected final Coordinator coordinator;
  protected final SegId segId;
  protected final TriggerByCheckCallback callback;

  protected final long requestId;
  private AtomicInteger sendCount;
  private List<SegmentMembership> memberships;
  private String className;
  private ReentrantLock lock;



  public GetMembershipCallbackCollector(Coordinator coordinator, int sendCount, SegId segId,
      long requestId,
      TriggerByCheckCallback callback) {
    this.coordinator = coordinator;
    this.sendCount = new AtomicInteger(sendCount);
    this.segId = segId;
    this.requestId = requestId;
    this.callback = callback;
    this.memberships = new ArrayList<>();
    this.lock = new ReentrantLock(true);

    this.className = "GetMembershipCallbackCollector";
  }

  @Override
  public void complete(Broadcastlog.PbGetMembershipResponse object) {
    SegmentMembership membership = PbRequestResponseHelper
        .buildMembershipFrom(object.getPbMembership());
    try {
      lock.lock();
      this.memberships.add(membership);
    } finally {
      lock.unlock();
    }
    logger.info("completed, ori:{} at:{} get membership:{}", requestId, segId, membership);
    if (this.sendCount.decrementAndGet() == 0) {
      nextProcess();
    }
  }

  @Override
  public void fail(Exception e) {
    logger.warn("failed, ori:{} at:{} get membership caught an exception:{}", requestId, segId, e);
    LoggerTracer.getInstance()
        .mark(requestId, this.className,
            "failed, ori:{} at:{} get membership caught an exception:{}",
            requestId, segId, e);
    if (this.sendCount.decrementAndGet() == 0) {
      nextProcess();
    }
  }



  public void nextProcess() {


    SegmentMembership highestMembership = null;
    if (this.memberships.isEmpty()) {
      logger.warn("ori:{} at:{} can not get membership any more, should retrieve from infocenter",
          requestId,
          segId);

      coordinator.getGetMembershipFromInfoCenterEngine()
          .putTask(new GetMembershipTask(callback.getVolumeId(), segId, requestId, null));
    } else {
      for (SegmentMembership membership : this.memberships) {

        if (highestMembership == null) {
          highestMembership = membership;
        } else if (highestMembership.compareTo(membership) < 0) {
          highestMembership = membership;
        }
      }
    }

    if (highestMembership != null) {
      logger.info("ori:{} at:{} get membership:{}", requestId, segId, highestMembership);
      coordinator
          .updateMembershipFromDatanode(callback.getVolumeId(), segId, highestMembership, callback);


      if (callback != null) {
        callback.resetNeedUpdateMembership();
      }
    }
  }
}
