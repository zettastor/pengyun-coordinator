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

package py.coordinator.worker;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lib.Coordinator;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.thrift.coordinator.service.UpdateVolumeOnExtendingRequest;
import py.volume.VolumeMetadata;


public class ExtendingVolumeUpdater extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(ExtendingVolumeUpdater.class);

  private final InformationCenterClientFactory icClientFactory;
  private final LaunchDriverParameters launchDriverParameters;

  private final BlockingQueue<UpdateVolumeOnExtendingRequest> requestQueue =
      new LinkedBlockingQueue<UpdateVolumeOnExtendingRequest>();

  private PydClientManager pydClientManager;

  private Coordinator coordinator;



  public ExtendingVolumeUpdater(final InformationCenterClientFactory icClientFactory,
      final LaunchDriverParameters launchDriverParameters) {
    super();
    this.icClientFactory = icClientFactory;
    this.launchDriverParameters = launchDriverParameters;
  }

  @Override
  public void run() {
    while (true) {
      try {
        UpdateVolumeOnExtendingRequest request = requestQueue.take();

        if (request instanceof ExitRequest) {
          logger.warn("going to exit ExtendingVolumeUpdater");
          return;
        }

        Long volumeId = request.getVolumeId();
        logger.warn("going to wait volume:{} if extended", volumeId);
        boolean extended = waitUntilVolumeExtended(volumeId);
        if (extended) {
          logger.warn("volume should be extended completely, update it:{}",
              launchDriverParameters.getVolumeId());
          coordinator.updateVolumeMetadataIfVolumeExtended(volumeId);

          logger.warn("extend volume, so we close all pyd client connections");

          pydClientManager.closeAllClientsForExtendVolume();
        } else {
          logger
              .warn("volume:{} has no changed or still not extend successfully, do nothing for now",
                  launchDriverParameters.getVolumeId());
        }
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }
    }
  }

  public void addNewRequest(UpdateVolumeOnExtendingRequest request) {
    requestQueue.offer(request);
  }

  public void close() {
    addNewRequest(new ExitRequest());
  }

  private boolean waitUntilVolumeExtended(Long volumeId) {
    final int waitInterval = 5000;
    final int waitTimeout = 20 * 60 * 1000;

    int tryRemaining = waitTimeout / waitInterval;
    while (tryRemaining-- > 0) {
      try {
        InformationCenterClientWrapper icClientWrapper = icClientFactory.build();

        VolumeMetadata volume = icClientWrapper
            .getVolumeByPagination(volumeId, SUPERADMIN_ACCOUNT_ID).getVolumeMetadata();
        if (null != volume && volume.getExtendingSize() == 0) {
          SpaceSavingVolumeMetadata currentVolume = coordinator.getVolumeMetaData(volumeId);
          if (currentVolume.getVolumeSize() < volume.getVolumeSize()) {
            logger.warn("volume:{} extended successfully", volumeId);
            return true;
          } else {
            if (currentVolume.getVolumeSize() != volume.getVolumeSize()) {
              Validate.isTrue(false,
                  "current volume size:" + currentVolume.getVolumeSize() + "get volume size:"
                      + volume
                      .getVolumeSize());
            }
            logger.warn("volume:{} still is:{} has no change", volumeId,
                currentVolume.getVolumeSize());
            return false;
          }
        }
      } catch (Exception e) {
        logger.warn("Caught an exception when wait for finishing extending volume {}",
            volumeId, e);
      }

      try {
        logger.warn("Wait for more {} ms to check extending volume {}", waitInterval,
            volumeId);
        Thread.sleep(waitInterval);
      } catch (InterruptedException e) {
        logger.warn("Caught an exception", e);
      }
    }
    logger.error("wait volume:{} extend timeout", volumeId);
    return false;
  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  private class ExitRequest extends UpdateVolumeOnExtendingRequest {

  }

}
