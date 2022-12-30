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

package py.coordinator.main;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.AfterClass;
import org.junit.Test;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.LimitQueue;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iounit.WriteIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.IoUnitContextImpl;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.volumeinfo.DummyVolumeInfoRetriever;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.icshare.BroadcastLogStatus;
import py.instance.DummyInstanceStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.volume.VolumeMetadata;

/**
 * cover all paths that the coordinator should deal with.
 */
@SuppressWarnings("unchecked")
public class CoordinatorTest extends TestBase {

  private static Coordinator coordinator;
  private long volumeId = RequestIdBuilder.get();

  private GenericThriftClientFactory<DataNodeService.Iface> clientFactory = mock(
      GenericThriftClientFactory.class);
  private GenericThriftClientFactory<DataNodeService.AsyncIface> asyncClientFactory = mock(
      GenericThriftClientFactory.class);
  private InstanceStore instanceStore;
  private DataNodeService.Iface syncClient = mock(DataNodeService.Iface.class);
  private DataNodeService.AsyncIface asyncClient = mock(DataNodeService.AsyncIface.class);

  private VolumeInfoRetriever volumeInfoRetriever;
  private CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
  private IoContextGenerator generator;
  private VolumeMetadata volumeMetadata;


  /**
   * xx.
   */
  public CoordinatorTest() throws Exception {
    super.init();
    volumeMetadata = TestUtils.generateVolumeMetadata();
    initInstanceStore();

    volumeMetadata.setVolumeSize(cfg.getSegmentSize() * 2);
    volumeInfoRetriever = new DummyVolumeInfoRetriever(volumeMetadata);
    when(clientFactory.generateSyncClient(any(EndPoint.class), anyLong(), anyInt()))
        .thenReturn(syncClient);
    when(asyncClientFactory.generateAsyncClient(any(EndPoint.class), anyInt()))
        .thenReturn(asyncClient);

    cfg.setMaxWriteDataSizePerRequest(15 * 1024 * 1024);
    cfg.setMaxReadDataSizePerRequest(256 * 1024);

    ByteBufAllocator allocator = new SimplePooledByteBufAllocator((int) cfg.getCachePoolSizeBytes(),
        cfg.getMediumPageSizeForPool(), cfg.getLittlePageSizeForPool(),
        cfg.getLargePageSizeForPool());
    coordinator = new Coordinator(instanceStore, clientFactory, asyncClientFactory, allocator);
    coordinator.getVolumeInfoHolderManager().setVolumeInfoRetrieve(volumeInfoRetriever);

    List<Long> volumeLayout = new ArrayList<Long>();
    volumeLayout.add(2L);
    generator = new IoContextGenerator(volumeId, 1024 * 1024, 1024, cfg.getPageWrappedCount(),
        volumeLayout);

  }

  @AfterClass
  public static void afterClass() throws Exception {
    coordinator.close();
  }

  @Test
  public void testStartAndStop() throws Exception {
    // Map<SegId, List<BroadcastLog>> mapSegIdToLogs = generateInitalLogs();
    // when(syncClient.getLatestLogsFromPrimary(any(GetLatestLogsRequestThrift.class)))
    // .thenReturn(NBDRequestResponseGenerator.generateGetLatestLogsResponse(mapSegIdToLogs));
    // try {
    // coordinator.open();
    // } catch (Exception e) {
    // logger.error("caught an exception", e);
    // assertTrue(false);
    // }
    //
    // List<IOUnitContext> contexts1 = generator.generateWriteIOContexts(1000, 8000);
    // assertTrue(contexts1.size() == 9);
    // List<IOUnitContext> contexts2 = generator.generateWriteIOContexts(1024 * 1025, 8000);
    // assertTrue(contexts2.size() == 8);
    //
    // IOUnitContextPacket callback1 = new IOUnitContextPacketImpl(contexts1, 0, RequestType.Write);
    // IOUnitContextPacket callback2 = new IOUnitContextPacketImpl(contexts2, 1, RequestType.Write);
    // coordinator.submit(callback1);
    // coordinator.submit(callback2);
    //
    // coordinator.close();
  }


  /**
   * xx.
   */
  public Map<SegId, List<BroadcastLog>> generateInitalLogs() throws Exception {
    Map<SegId, List<BroadcastLog>> mapSegIdToLogs = new HashMap<SegId, List<BroadcastLog>>();
    SpaceSavingVolumeMetadata volumeMetadata = volumeInfoRetriever
        .getVolume(RequestIdBuilder.get());
    Map<Integer, LimitQueue<SegmentMembership>> mapIndexToMembership = volumeMetadata
        .getMemberships();
    for (Integer index : mapIndexToMembership.keySet()) {
      SegId segId = volumeMetadata.getSegId(index);
      List<BroadcastLog> logs = new ArrayList<BroadcastLog>();
      IoUnitContext ioContext = new IoUnitContextImpl(null,
          new WriteIoUnitImpl(index, -1, 0L, 0, null));
      logs.add(new BroadcastLog(0, ioContext, 0).setStatus(BroadcastLogStatus.Committed));
      mapSegIdToLogs.put(segId, logs);
    }

    return mapSegIdToLogs;
  }


  /**
   * xx.
   */
  public void initInstanceStore() {
    int startPort = 1234;
    instanceStore = new DummyInstanceStore();
    for (Entry<Integer, LimitQueue<SegmentMembership>> membership : volumeMetadata.getMemberships()
        .entrySet()) {
      for (InstanceId instanceId : membership.getValue().getLast().getMembers()) {
        Instance instance = instanceStore.get(instanceId);
        if (instance == null) {
          instance = new Instance(instanceId, "DataNode", InstanceStatus.HEALTHY,
              new EndPoint("10.0.1.127", startPort));
          instance.putEndPointByServiceName(PortType.IO, new EndPoint("10.0.1.128", startPort));
          instanceStore.save(instance);
          startPort++;
        }
      }
    }
  }
}
