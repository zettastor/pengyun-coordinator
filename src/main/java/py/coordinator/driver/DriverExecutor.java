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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.exception.FailedToStartDriverException;



public class DriverExecutor implements Runnable {

  private static Logger logger = LoggerFactory.getLogger(DriverExecutor.class);

  private Driver driver;

  private List<Exception> exceptionCollector;

  public List<Exception> getExceptionCollector() {
    return exceptionCollector;
  }

  public void setExceptionCollector(List<Exception> exceptionCollector) {
    this.exceptionCollector = exceptionCollector;
  }

  public Driver getDriver() {
    return driver;
  }

  public void setDriver(Driver driver) {
    this.driver = driver;
  }

  @Override
  public void run() {
    try {

      driver.launch();
    } catch (FailedToStartDriverException e) {
      logger.error("fail to start the driver ", e);
      exceptionCollector.add(e);
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
    logger.warn("start driver successfully");
  }

}
