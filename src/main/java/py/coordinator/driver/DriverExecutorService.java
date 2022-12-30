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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.exception.FailedToStartDriverException;
import py.drivercontainer.utils.DriverContainerUtils;


public class DriverExecutorService {

  private static Logger logger = LoggerFactory.getLogger(DriverExecutorService.class);

  private Driver driver;

  private List<Exception> exceptionCollector;

  private int launchDriverTimeoutMs;

  public DriverExecutorService() {
    exceptionCollector = new ArrayList<>();
  }

  public Driver getDriver() {
    return driver;
  }

  public void setDriver(Driver driver) {
    this.driver = driver;
  }

  public void setLaunchDriverTimeoutMs(int launchDriverTimeoutMs) {
    logger.warn("try to set launch driver timeout:{}ms", launchDriverTimeoutMs);
    this.launchDriverTimeoutMs = launchDriverTimeoutMs;
  }



  public void execute() throws FailedToStartDriverException {
    DriverExecutor driverExecutor = new DriverExecutor();
    driverExecutor.setDriver(driver);
    driverExecutor.setExceptionCollector(exceptionCollector);

    String hostname = driver.getDriverConfiguration().getHostName();
    int port = driver.getDriverConfiguration().getPort();
    if (!DriverContainerUtils.isPortAvailable(hostname, port)) {
      logger.error("host name:{} and port:{} is in used, can not launch driver", hostname, port);
      throw new FailedToStartDriverException(
          "port " + port + " is already in used.");
    }


    Thread thread = new Thread(driverExecutor);
    thread.start();

    long executorStartTime = System.currentTimeMillis();
    while (DriverContainerUtils.isPortAvailable(hostname, port)) {
      if (exceptionCollector.size() != 0) {
        logger.error("launch driver at:[{}:{}] caught exception", hostname, port);
        throw new FailedToStartDriverException();
      }
      long costTimeMs = System.currentTimeMillis() - executorStartTime;
      if (costTimeMs > launchDriverTimeoutMs) {
        logger.error("launch driver at:[{}:{}] cost time:{}ms", hostname, port, costTimeMs);
        throw new FailedToStartDriverException();
      }
      try {
        logger.warn("launch driver at:[{}:{}] still not startup, wait a moment", hostname, port);
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new FailedToStartDriverException();
      }
    }

    if (exceptionCollector.size() != 0) {
      logger.error("launch driver at:[{}:{}] caught exception", hostname, port);
      throw new FailedToStartDriverException();
    } else {
      logger.warn("launch driver at:[{}:{}] looks like startup, do next step work", hostname, port);
    }
  }

}
