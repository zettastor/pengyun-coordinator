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
