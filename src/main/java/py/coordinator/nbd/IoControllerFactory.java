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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.IoLimitManagerImpl.IntValue;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class IoControllerFactory implements WorkerFactory {

  private static final Logger logger = LoggerFactory.getLogger(IoController.class);

  private BlockingQueue<String> tokenBucket;

  private AtomicInteger countIO;

  private InstanceStore instanceStore;

  private IntValue limitedIops;

  private boolean upperLimit;

  private long volumeId;

  private CoordinatorClientFactory coordinatorClientFactory;

  private int interval;


  
  public IoControllerFactory(IntValue limitedIops, boolean upperLimit, int interval) {
    this.limitedIops = limitedIops;
    this.upperLimit = upperLimit;
    this.interval = interval;
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

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public IntValue getLimitedIops() {
    return limitedIops;
  }

  public void setLimitedIops(IntValue limitedIops) {
    this.limitedIops = limitedIops;
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

  @Override
  public Worker createWorker() {
    IoController worker = new IoController(limitedIops, upperLimit, interval);
    worker.setTokenBucket(tokenBucket);
    worker.setCountIO(countIO);
    worker.setInstanceStore(instanceStore);
    worker.setVolumeId(volumeId);
    worker.setCoordinatorClientFactory(coordinatorClientFactory);
    return worker;
  }
}
