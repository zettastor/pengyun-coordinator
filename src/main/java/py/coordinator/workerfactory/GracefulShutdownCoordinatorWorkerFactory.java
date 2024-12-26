
package py.coordinator.workerfactory;

import py.coordinator.service.CoordinatorImpl;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class GracefulShutdownCoordinatorWorkerFactory implements WorkerFactory {

  private CoordinatorImpl coordinatorImpl;

  public GracefulShutdownCoordinatorWorkerFactory(CoordinatorImpl coordinatorImpl) {
    this.coordinatorImpl = coordinatorImpl;
  }

  @Override
  public Worker createWorker() {
    GracefulShutdownCoordinatorWorker worker = new GracefulShutdownCoordinatorWorker(
        coordinatorImpl);
    return worker;
  }
}
