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

package py.coordinator.workerfactory;

import org.apache.commons.lang3.Validate;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.VolumeInfoHolderManager;
import py.coordinator.nbd.PydClientManager;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class DriverInfoReportWorkerFactory implements WorkerFactory {

  private InstanceStore instanceStore;
  private InstanceId driverContainerId;
  private NbdConfiguration nbdConfiguration;
  private PydClientManager pydClientManager;
  private VolumeInfoHolderManager volumeInfoHolderManager;


  
  public DriverInfoReportWorkerFactory(InstanceStore instanceStore, InstanceId driverContainerId,
      NbdConfiguration nbdConfiguration, PydClientManager pydClientManager,
      VolumeInfoHolderManager volumeInfoHolderManager) {
    Validate.notNull(instanceStore);
    Validate.notNull(driverContainerId);
    this.instanceStore = instanceStore;
    this.driverContainerId = driverContainerId;
    this.nbdConfiguration = nbdConfiguration;
    this.pydClientManager = pydClientManager;
    this.volumeInfoHolderManager = volumeInfoHolderManager;
  }

  @Override
  public Worker createWorker() {
    DriverInfoReportWorker driverInfoReportWorker = new DriverInfoReportWorker(instanceStore,
        driverContainerId,
        nbdConfiguration, pydClientManager, this.volumeInfoHolderManager);
    return driverInfoReportWorker;
  }
}
