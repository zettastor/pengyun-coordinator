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

package py.coordinator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.Coordinator;
import py.coordinator.volumeinfo.VolumeInfoRetrieverImpl;
import py.coordinator.worker.ExtendingVolumeUpdater;
import py.drivercontainer.exception.FailedToBuildCoordinatorException;
import py.exception.StorageException;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.thrift.datanode.service.DataNodeService;


public class CoordinatorBuilder {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorBuilder.class);

  private InformationCenterClientFactory informationCenterClientFactory;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;
  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  private InstanceStore instanceStore;
  private long volumeId;
  private long accountId;
  private int snapshotId;
  private long driverContainerId;
  private ExtendingVolumeUpdater extendingVolumeUpdater;

  public CoordinatorBuilder() {
   
  }

  
  public static Coordinator build(Long volumeId, int snapshotId, Long accountId,
      InstanceStore instanceStore)
      throws FailedToBuildCoordinatorException {
    final CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
    Validate.notNull(instanceStore);
    InformationCenterClientFactory informationCenterClientFactory =
        new InformationCenterClientFactory(
            1);
    informationCenterClientFactory.setInstanceName(PyService.INFOCENTER.getServiceName());
    informationCenterClientFactory.setInstanceStore(instanceStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory =
        GenericThriftClientFactory
            .create(DataNodeService.Iface.class)
            .withMaxChannelPendingSizeMb(cfg.getMaxChannelPendingSize());

    GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory =
        GenericThriftClientFactory
            .create(DataNodeService.AsyncIface.class)
            .withMaxChannelPendingSizeMb(cfg.getMaxChannelPendingSize());

    VolumeInfoRetrieverImpl volumeInfoRetriever = new VolumeInfoRetrieverImpl();
    volumeInfoRetriever.setInfoCenterClientFactory(informationCenterClientFactory);

    ByteBufAllocator allocator = new SimplePooledByteBufAllocator((int) cfg.getCachePoolSizeBytes(),
        cfg.getMediumPageSizeForPool(), cfg.getLittlePageSizeForPool(),
        cfg.getLargePageSizeForPool());
    Coordinator coordinator = new Coordinator(instanceStore, dataNodeSyncClientFactory,
        dataNodeAsyncClientFactory,
        allocator);
    coordinator.setInformationCenterClientFactory(informationCenterClientFactory);

    
    try {
      coordinator.open(volumeId, snapshotId);
    } catch (StorageException e) {
      logger.error("caught an exception", e);
      throw new RuntimeException();
    }
    return coordinator;
  }


  
  @SuppressWarnings("unchecked")
  public Coordinator build() throws FailedToBuildCoordinatorException {
    try {
     
     
      CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
      ByteBufAllocator allocator = PooledByteBufAllocatorWrapper.INSTANCE;
      Coordinator coordinator = new Coordinator(instanceStore, dataNodeSyncClientFactory,
          dataNodeAsyncClientFactory, allocator);

      coordinator.setDriverContainerId(new InstanceId(driverContainerId));
      coordinator.setInformationCenterClientFactory(informationCenterClientFactory);
      extendingVolumeUpdater.setCoordinator(coordinator);
      logger.warn("successfully instantiated a coordinator, direct buffer:{}",
          PlatformDependent.directBufferPreferred());

      return coordinator;
    } catch (Exception e) {
      logger.error("caught an exception when instantiating a coordinator", e);
      throw new FailedToBuildCoordinatorException(e);
    }
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public long getInstanceId() {
    return driverContainerId;
  }

  public void setInstanceId(long instanceId) {
    this.driverContainerId = instanceId;
  }

  public void setExtendingVolumeUpdater(ExtendingVolumeUpdater extendingVolumeUpdater) {
    this.extendingVolumeUpdater = extendingVolumeUpdater;
  }

  public void setDataNodeSyncClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory) {
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  public void setDataNodeAsyncClientFactory(
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory) {
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }
}
