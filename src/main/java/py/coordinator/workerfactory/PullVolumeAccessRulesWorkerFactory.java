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

import py.coordinator.configuration.NbdConfiguration;
import py.icshare.qos.IoLimitScheduler;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class PullVolumeAccessRulesWorkerFactory implements WorkerFactory {

  private InstanceStore instanceStore;
  private IoLimitScheduler ioLimitScheduler;
  private NbdConfiguration nbdConfiguration;



  public PullVolumeAccessRulesWorkerFactory(InstanceStore instanceStore,
      NbdConfiguration nbdConfiguration, IoLimitScheduler ioLimitScheduler) {
    this.instanceStore = instanceStore;
    this.nbdConfiguration = nbdConfiguration;
    this.ioLimitScheduler = ioLimitScheduler;
  }

  @Override
  public Worker createWorker() {
    PullVolumeAccessRulesWorker pullVolumeAccessRulesWorker = new PullVolumeAccessRulesWorker(
        instanceStore,
        nbdConfiguration, ioLimitScheduler);
    return pullVolumeAccessRulesWorker;
  }
}
