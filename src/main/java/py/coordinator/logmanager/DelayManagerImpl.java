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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.DelayManager;
import py.coordinator.lib.Coordinator;
import py.coordinator.task.BogusDelayed;
import py.coordinator.task.CreateSegmentContext;
import py.coordinator.task.ResendRequest;
import py.coordinator.task.UpdateMembershipRequest;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;


public class DelayManagerImpl implements DelayManager {

  public static final long DEFAULT_UPDATE_MEMBERSHIP_IF_NO_IO = 60000;
  private static final Logger logger = LoggerFactory.getLogger(DelayManagerImpl.class);
  private static final long DELAY_EXECUTION_TIME_MS = 500;
  private final Thread requestPullerThread;
  private DelayQueue<Delayed> delayQueue;
  private boolean pause;
  private Coordinator coordinator;
  private VolumeInfoRetriever volumeInfoRetriever;

 

  
  public DelayManagerImpl(Coordinator coordinator, VolumeInfoRetriever volumeInfoRetriever) {
    this.coordinator = coordinator;
    this.delayQueue = new DelayQueue<>();
    this.volumeInfoRetriever = volumeInfoRetriever;
    requestPullerThread = new Thread("Delay-Puller") {
      @Override
      public void run() {
        try {
          startJob();
        } catch (Throwable t) {
          logger.error("caught an exception", t);
        }
      }
    };
    requestPullerThread.start();
    pause = false;
  }

  @Override
  public void put(Delayed delayed) {
    delayQueue.put(delayed);
  }


  
  public void startJob() {
    List<Delayed> requestsRetrieved = new ArrayList<Delayed>();
    while (true) {
      try {
        if (pause) {
          Thread.sleep(DELAY_EXECUTION_TIME_MS);
          continue;
        }

        // wait forever until an available task available to execute
        requestsRetrieved.clear();
        delayQueue.drainTo(requestsRetrieved);
        if (requestsRetrieved.isEmpty()) {
          requestsRetrieved.add(delayQueue.take());
        }

        // process re-send requests pulled from delay queue
        for (Delayed request : requestsRetrieved) {
          try {
            if (request instanceof BogusDelayed) {
              logger.warn("exit resend thread");
              return;
            } else if (request instanceof ResendRequest) {
              processResendRequest((ResendRequest) request);
            } else if (request instanceof CreateSegmentContext) {
              processCreateSegment((CreateSegmentContext) request);
            } else if (request instanceof UpdateMembershipRequest) {
              processUpdateMembership((UpdateMembershipRequest) request);
            } else {
              logger.error("not support this type delay: {}", request);
            }
          } catch (Exception e) {
            logger.error("process request:{} failed", request, e);
          }
        }
      } catch (Exception e) {
        logger.error("request puller caught an exception ", e);
      }
    }
  }

  @Override
  public void pause() {
    pause = true;
  }

  @Override
  public void start() {
    pause = false;
  }

  @Override
  public void stop() {
    try {
      logger.warn("try to stop delay manager");
      delayQueue.offer(new BogusDelayed());
      requestPullerThread.join(1000);
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }
  }

  protected void processResendRequest(ResendRequest resendRequest) {
    IoContextManager manager = resendRequest.getIoContextManager();
    try {
      if (manager instanceof ReadIoContextManager) {
        coordinator.resendRead(manager, resendRequest.needUpdateMembership());
      } else if (manager instanceof WriteIoContextManager) {
        coordinator.resendWrite(manager, resendRequest.needUpdateMembership());
      } else {
        logger.error("unknown request type, ori:{} at:{}", manager.getRequestId(),
            manager.getSegId());
      }
    } catch (Exception e) {
      logger.error("try to resend IO request, ori:{} at:{} failed", manager.getRequestId(),
          manager.getSegId(), e);
     
      if (manager.isExpired()) {
        manager.doResult();
      } else {
       
        logger.error("ori:{} something we don't know happened, should set resend delay to:{}",
            manager.getRequestId(), AbstractIoContextManager.DEFAULT_DELAY_MS);
        resendRequest.updateDelay(AbstractIoContextManager.DEFAULT_DELAY_MS);
        put(resendRequest);
      }
    }
  }

  private void processCreateSegment(CreateSegmentContext request) {
    logger.warn("now to create segment for an uncompleted volume");
    Long volumeId = request.getVolumeId();
    int segmentIndex = request.getStartSegmentIndex();
    if (!request.isCreateRequestSent()) {
      try {
        boolean succeed = volumeInfoRetriever
            .createSegment(volumeId, segmentIndex, request.getVolumeType(), request.getCacheType(),
                request.getSegmentCount(), request.getDomainId(), request.getStoragePoolId());
        request.setCreateRequestSent(succeed);
      } catch (Exception e) {
        logger.error("try to create segment failed, don't worry, I'll try again later", e);
      }
    } else {
      try {
        SpaceSavingVolumeMetadata volume = volumeInfoRetriever.getVolume(volumeId);
        SegId segId = volume.getSegId(segmentIndex);
       
        if (segId != null) {
          logger.warn("segment successfully created !!");
          coordinator.segmentCreated(volumeId, segmentIndex);
          return;
        } else {
          logger.warn("segment not successfully created yet, try again later");
        }
      } catch (Exception e) {
        logger.warn("cannot check if segment created, don't worry, I'll try again later", e);
      }
    }

    // It's not done, put it back to the queue
    put(request.clone());
  }

  private void processUpdateMembership(UpdateMembershipRequest request) {
    try {
      coordinator.processUpdateMembership();
    } catch (Exception e) {
      logger.error("caught an exception when try get membership", e);
    } finally {
     
      request.refreshDelay();
      put(request);
    }
  }

  public VolumeInfoRetriever getVolumeInfoRetriever() {
    return volumeInfoRetriever;
  }

  public void setVolumeInfoRetriever(VolumeInfoRetriever volumeInfoRetriever) {
    this.volumeInfoRetriever = volumeInfoRetriever;
  }
}
