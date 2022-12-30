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
