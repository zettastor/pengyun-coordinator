
package py.coordinator.workerfactory;

import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.PydClientManager;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class PydHeartbeatWorkerFactory implements WorkerFactory {

  private final PydClientManager pydClientManager;
  private StorageDriver storageDriver;

  public PydHeartbeatWorkerFactory(PydClientManager pydClientManager, StorageDriver storageDriver) {
    this.pydClientManager = pydClientManager;
    this.storageDriver = storageDriver;
  }

  @Override
  public Worker createWorker() {
    PydHeartbeatWorker worker = new PydHeartbeatWorker(this.pydClientManager, this.storageDriver);
    return worker;
  }
}
