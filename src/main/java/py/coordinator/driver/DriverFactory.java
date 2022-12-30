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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.CoordinatorBuilder;
import py.coordinator.driver.Driver.Type;
import py.coordinator.service.CoordinatorImpl;
import py.drivercontainer.driver.DriverAppContext;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.DriverTypeNotSupportedException;
import py.icshare.qos.IoLimitScheduler;
import py.infocenter.client.InformationCenterClientFactory;


public class DriverFactory {

  private static Logger logger = LoggerFactory.getLogger(DriverFactory.class);

  protected LaunchDriverParameters launchDriverParameters;

  protected CoordinatorBuilder coordinatorBuilder;

  protected InformationCenterClientFactory informationCenterClientFactory;

  protected IoLimitScheduler ioLimitScheduler;

  public DriverFactory(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }



  public Driver build(Type type, CoordinatorImpl coordinatorImpl,
      DriverAppContext pyAppContext)
      throws DriverTypeNotSupportedException {
    Driver driver = null;

    switch (type) {
      case NBD:
        driver = new NbdDriver(launchDriverParameters, false, coordinatorImpl,
            pyAppContext);
        break;
      case JSCSI:
        driver = new JscsiDriver(launchDriverParameters);
        break;
      case ISCSI:
        driver = new NbdDriver(launchDriverParameters, true, coordinatorImpl,
            pyAppContext);
        break;
      default:
        throw new DriverTypeNotSupportedException();
    }

    driver.setCoordinatorBuilder(coordinatorBuilder);
    driver.setInformationCenterClientFactory(informationCenterClientFactory);
    driver.setIoLimitScheduler(ioLimitScheduler);
    return driver;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public void setCoordinatorBuilder(CoordinatorBuilder coordinatorBuilder) {
    this.coordinatorBuilder = coordinatorBuilder;
  }

  public IoLimitScheduler getIoLimitScheduler() {
    return ioLimitScheduler;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }

}
