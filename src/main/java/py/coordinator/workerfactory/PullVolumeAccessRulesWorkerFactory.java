

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
