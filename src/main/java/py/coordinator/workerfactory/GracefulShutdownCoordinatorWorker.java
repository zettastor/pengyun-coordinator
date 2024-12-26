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
