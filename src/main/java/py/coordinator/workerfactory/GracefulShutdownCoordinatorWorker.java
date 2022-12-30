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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.service.CoordinatorImpl;
import py.periodic.Worker;



public class GracefulShutdownCoordinatorWorker implements Worker {

  private static final Logger logger = LoggerFactory
      .getLogger(GracefulShutdownCoordinatorWorker.class);

  private CoordinatorImpl coordinatorImpl;
  private PydClientManager pydClientManager;

  public GracefulShutdownCoordinatorWorker(CoordinatorImpl coordinatorImpl) {
    this.coordinatorImpl = coordinatorImpl;
    this.pydClientManager = this.coordinatorImpl.getPydClientManager();
  }

  @Override
  public void doWork() throws Exception {

    if (pydClientManager.getAllClients().size() > 0) {
      logger.warn("gracefully shutting down, checking pydClientManager:{}",
          pydClientManager.getAllClients());
      return;
    }

    logger.warn("gracefully shutting down, now no client exist. going to shutdown coordinator.");
    coordinatorImpl.shutdown();
  }
}
