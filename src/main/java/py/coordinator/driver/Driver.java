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
