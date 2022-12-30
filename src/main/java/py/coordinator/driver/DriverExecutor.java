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
