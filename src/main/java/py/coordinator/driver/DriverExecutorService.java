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
