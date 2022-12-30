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
