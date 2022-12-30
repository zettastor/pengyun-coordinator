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
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.FailedToStartDriverException;
import py.icshare.qos.IoLimitScheduler;
import py.infocenter.client.InformationCenterClientFactory;



public abstract class Driver {

  protected static Logger logger = LoggerFactory.getLogger(Driver.class);
  protected LaunchDriverParameters launchDriverParameters;
  protected CoordinatorBuilder coordinatorBuilder;
  protected InformationCenterClientFactory informationCenterClientFactory;
  protected IoLimitScheduler ioLimitScheduler;

  public Driver(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }

  public static void setLogger(Logger logger) {
    Driver.logger = logger;
  }

  public CoordinatorBuilder getCoordinatorBuilder() {
    return coordinatorBuilder;
  }

  public void setCoordinatorBuilder(CoordinatorBuilder coordinatorBuilder) {
    this.coordinatorBuilder = coordinatorBuilder;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public LaunchDriverParameters getDriverConfiguration() {
    return launchDriverParameters;
  }

  public void setDriverConfiguration(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
  }

  public IoLimitScheduler getIoLimitScheduler() {
    return ioLimitScheduler;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }

  public abstract void launch() throws FailedToStartDriverException;

  public enum Type {
    NBD, JSCSI, ISCSI
  }
}
