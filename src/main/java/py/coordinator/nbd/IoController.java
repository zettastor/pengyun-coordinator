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

package py.coordinator.nbd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.coordinator.nbd.IoLimitManagerImpl.IntValue;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.ResetSlowLevelRequest;
import py.thrift.coordinator.service.SlowDownRequest;


public class IoController implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(IoController.class);

  private int interval;

  
  private boolean upperLimit;

  private BlockingQueue<String> tokenBucket;

  private AtomicInteger countIO;

  private IntValue limitedIops;

  private InstanceStore instanceStore;

  private Long volumeId;

  private CoordinatorClientFactory coordinatorClientFactory;

  private int slowDownCount = 0;

  private List<Integer> ioQueue;


  
  public IoController(IntValue limitedIops, boolean upperLimit, int interval) {
    this.limitedIops = limitedIops;
    this.upperLimit = upperLimit;
    this.interval = interval;
    ioQueue = new ArrayList<Integer>();
  }

  @Override
  public void doWork() {
    if (upperLimit) {
      processUpperLimit();
    } else {
      try {
        processLowerLimit();
      } catch (Exception e) {
        logger.error("Exception catch", e);
      }
    }
  }

  private void processUpperLimit() {
    Validate.notNull(tokenBucket);
    int tokensPutEveryInterval = limitedIops.getValue() / (1000 / interval);
    try {
      tokensPutEveryInterval = limitedIops.getValue() / (1000 / interval);
      tokenBucket.clear();
      logger.debug("after clear, now to put token, num: {}, size: {} ", tokensPutEveryInterval,
          tokenBucket.size());
      for (int i = 0; i < tokensPutEveryInterval; i++) {
        if (tokenBucket.size() < limitedIops.getValue()) {
          tokenBucket.put("token");
        }
      }
    } catch (InterruptedException e) {
      logger.info("controller interrupted");
    }
  }

  private void processLowerLimit()
      throws GenericThriftClientFactoryException, TException, InterruptedException {

    Validate.notNull(countIO);
    Validate.notNull(coordinatorClientFactory);
    Validate.notNull(instanceStore);
    Validate.notNull(volumeId);

    logger.debug("real time IOPS {}", countIO.get());
   
    int realTimeIops = countIO.getAndSet(0) * (1000 / interval);
    ioQueue.add(realTimeIops);
    if (ioQueue.size() > 10) {
      ioQueue.remove(0);
    }

    int sum = 0;
    for (int i = 0; i < ioQueue.size(); i++) {
      sum += ioQueue.get(i);
    }

    int averageIops = sum / ioQueue.size();

    if (averageIops < limitedIops.getValue() || (averageIops - limitedIops.getValue() > 100
        && slowDownCount > 0)) {
      for (Instance coordinatorInstance : instanceStore
          .getAll(PyService.COORDINATOR.getServiceName(),
              InstanceStatus.HEALTHY)) {
        Coordinator.Iface client = coordinatorClientFactory
            .build(coordinatorInstance.getEndPoint())
            .getClient();
        if (averageIops > 10) {
          SlowDownRequest request = new SlowDownRequest();
          request.setRequestId(RequestIdBuilder.get());
          if (averageIops < limitedIops.getValue()) {
            slowDownCount++;
            logger.debug("iops : {} slowDownCount : {}", averageIops, slowDownCount);
            request.setSlowDownLevel(1);
          } else {
           
            slowDownCount--;
            logger.debug("iops : {} slowDownCount : {}", averageIops, slowDownCount);
            request.setSlowDownLevel(-1);
          }
          request.setVolumeId(volumeId);
          client.slowDownExceptFor(request);
        } else {
          ResetSlowLevelRequest resetRequest = new ResetSlowLevelRequest();
          resetRequest.setRequestId(RequestIdBuilder.get());
          resetRequest.setVolumeId(volumeId);
          int retryTime = 3;
          while (retryTime > 0) {
            try {
              client.resetSlowLevel(resetRequest);
              break;
            } catch (Exception e) {
              retryTime--;
              Thread.sleep(1000);
            }
          }

        }
      }
    }
  }

  public BlockingQueue<String> getTokenBucket() {
    return tokenBucket;
  }

  public void setTokenBucket(BlockingQueue<String> tokenBucket) {
    this.tokenBucket = tokenBucket;
  }

  public AtomicInteger getCountIO() {
    return countIO;
  }

  public void setCountIO(AtomicInteger countIO) {
    this.countIO = countIO;
  }

  public IntValue getLimitedIops() {
    return limitedIops;
  }

  public void setLimitedIops(IntValue limitedIops) {
    this.limitedIops = limitedIops;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }

}
