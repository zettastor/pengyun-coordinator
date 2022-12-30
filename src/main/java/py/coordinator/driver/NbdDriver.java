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

package py.coordinator.driver;

import java.nio.file.Paths;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.common.struct.EndPoint;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.Coordinator;
import py.coordinator.nbd.NbdServer;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.service.CoordinatorImpl;
import py.driver.DriverType;
import py.drivercontainer.driver.DriverAppContext;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.FailedToStartDriverException;
import py.netty.memory.PooledByteBufAllocatorWrapper;


public class NbdDriver extends Driver {

  private static final Logger logger = LoggerFactory.getLogger(NbdDriver.class);

  
  private boolean usedInIscsi = false;
  private CoordinatorImpl coordinatorImpl;
  private DriverAppContext pyAppContext;


  
  public NbdDriver(LaunchDriverParameters launchDriverParameters, boolean usedInIscsi,
      CoordinatorImpl coordinatorImpl, DriverAppContext pyAppContext) {
    super(launchDriverParameters);
    this.coordinatorImpl = coordinatorImpl;
    this.pyAppContext = pyAppContext;
    this.usedInIscsi = usedInIscsi;
  }

  @Override
  public void launch() throws FailedToStartDriverException {
    logger.warn("Going to launch nbd driver ...");

    // initialize coordinator
    Coordinator coordinator;
    try {
      Validate.isTrue(launchDriverParameters.getDriverContainerInstanceId() != 0);
      Validate.isTrue(launchDriverParameters.getInstanceId() != 0);
      coordinator = coordinatorBuilder.build();
      coordinator.setMyInstanceId(launchDriverParameters.getInstanceId());
      coordinator.setTrueInstanceId(pyAppContext.getInstanceId());
      coordinator
          .open(launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId());
    } catch (Exception e) {
      logger.error("Failed to initialize coordinator due to exception", e);
      throw new FailedToStartDriverException(e);
    }
    try {
     
     
     

      @SuppressWarnings("resource") ApplicationContext appContext =
          new AnnotationConfigApplicationContext(
              NbdConfiguration.class);
      NbdConfiguration nbdConfig = appContext.getBean(NbdConfiguration.class);
      nbdConfig.setDriverContainerId(launchDriverParameters.getDriverContainerInstanceId());
      nbdConfig.setEndpoint(
          new EndPoint(launchDriverParameters.getHostName(), launchDriverParameters.getPort()));
      nbdConfig.setVolumeId(launchDriverParameters.getVolumeId());
      nbdConfig.setInformationCenterClientFactory(informationCenterClientFactory);
      nbdConfig.setSnapshotId(launchDriverParameters.getSnapshotId());
      nbdConfig.setDriverType(launchDriverParameters.getDriverType());
      nbdConfig.setDriverId(launchDriverParameters.getInstanceId());

     
      logger.warn("going to get volume access rules from info center");
      nbdConfig.getVolumeAccessRulesFromInfoCenter(launchDriverParameters.getVolumeId());
      logger.warn("after get volume access rules:{} from info center",
          nbdConfig.getVolumeAccessRules());

      coordinatorImpl.setNbdConfiguration(nbdConfig);
     
      coordinatorImpl.setCoordinator(coordinator);
      logger.warn("CoordinatorImpl:[{}], coordinator port:[{}]", coordinatorImpl,
          launchDriverParameters.getCoordinatorPort());
      PydClientManager pydClientManager = new PydClientManager(
          nbdConfig.getHeartbeatTimeIntervalAfterIoRequestMs(), true,
          nbdConfig.getReaderIdleTimeoutSec(),
          coordinator);
      NbdServer server = new NbdServer(nbdConfig, coordinator, pydClientManager);
     
      server.setAllocator(PooledByteBufAllocatorWrapper.INSTANCE);
      server.setIoLimitScheduler(ioLimitScheduler);
      coordinatorImpl.setNbdServer(server);
      coordinatorImpl.setPydClientManager(pydClientManager);
      coordinatorImpl.initPeriodicWork();

      String dir = Paths.get(System.getProperty("user.dir"), DriverType.NBD.name()).toString();

      coordinator.startOutput(dir, String.valueOf(launchDriverParameters.getVolumeId()),
          launchDriverParameters.getHostName());
      server.start();
      logger.warn("Coordinator's current appContext:{}, nbd server config:{}", pyAppContext,
          nbdConfig);
    } catch (Exception e) {
      logger.error("failed to launch nbd driver ", e);
      throw new FailedToStartDriverException(e);
    }
  }
}
