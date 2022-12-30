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

package py.coordinator.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.lib.AsyncIoCallBack;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.IoSeparatorImpl;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.volumeinfo.DummyVolumeInfoRetriever;
import py.coordinator.volumeinfo.VolumeInfoRetriever;
import py.exception.StorageException;
import py.instance.DummyInstanceStore;
import py.instance.InstanceStore;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class IoSeparatorTest extends TestBase {

  private final int maxWriteSize = 1280;
  private final int maxReadSize = 640;
  private final int pageSize = 128;
  private final int segmentSize = 1024 * 1024 * 16;
  CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
  private IoSeparatorImpl ioSeparator;
  private IoContextGenerator ioContextGenerator;
  private List<IoUnitContextPacket> submitCallbacks;
  private List<IoUnitContextPacket> failCallbacks;
  private Random random = new Random();
  private long volumeId = RequestIdBuilder.get();
  private DataNodeService.Iface syncClient = mock(DataNodeService.Iface.class);
  private DataNodeService.AsyncIface asyncClient = mock(DataNodeService.AsyncIface.class);
  private GenericThriftClientFactory<DataNodeService.Iface> clientFactory = mock(
      GenericThriftClientFactory.class);
  private GenericThriftClientFactory<DataNodeService.AsyncIface> asyncClientFactory = mock(
      GenericThriftClientFactory.class);
  private VolumeInfoRetriever volumeInfoRetriever;
  private InstanceStore instanceStore;
  private VolumeMetadata volumeMetadata;


  /**
   * xx.
   */
  public IoSeparatorTest() throws Exception {
    super.init();
    ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
    submitCallbacks = new ArrayList<>();
    failCallbacks = new ArrayList<>();

    volumeMetadata = TestUtils.generateVolumeMetadata();
    volumeMetadata.setVolumeSize(cfg.getSegmentSize() * 2);
    volumeInfoRetriever = new DummyVolumeInfoRetriever(volumeMetadata);
    when(clientFactory.generateSyncClient(any(EndPoint.class), anyLong(), anyInt()))
        .thenReturn(syncClient);
    when(asyncClientFactory.generateAsyncClient(any(EndPoint.class), anyInt()))
        .thenReturn(asyncClient);
    instanceStore = new DummyInstanceStore();
  }

  @Test
  public void testBase() throws Exception {
    ByteBufAllocator allocator = new SimplePooledByteBufAllocator((int) cfg.getCachePoolSizeBytes(),
        cfg.getMediumPageSizeForPool(), cfg.getLittlePageSizeForPool(),
        cfg.getLargePageSizeForPool());
    ioSeparator = new IoSeparator1(
        new Storage1(instanceStore, clientFactory, asyncClientFactory, allocator), maxReadSize,
        maxWriteSize);
    List<IoUnitContext> contexts;

    // generate read
    submitCallbacks.clear();
    int count = maxReadSize / pageSize;
    int pageCount = random.nextInt(count) + 1;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("read context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Read);
    assertTrue(submitCallbacks.size() == 1);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));

    // generate write
    submitCallbacks.clear();
    count = maxWriteSize / pageSize;
    pageCount = random.nextInt(count) + 1;
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("write context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Write);
    assertTrue(submitCallbacks.size() == 1);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));
  }

  @Test
  public void testSplit() throws Exception {
    ByteBufAllocator allocator = new SimplePooledByteBufAllocator((int) cfg.getCachePoolSizeBytes(),
        cfg.getMediumPageSizeForPool(), cfg.getLittlePageSizeForPool(),
        cfg.getLargePageSizeForPool());
    ioSeparator = new IoSeparator1(
        new Storage1(instanceStore, clientFactory, asyncClientFactory, allocator), maxReadSize,
        maxWriteSize);
    List<IoUnitContext> contexts;

    // generate read
    submitCallbacks.clear();
    int pageCount = 36;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("read context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Read);
    assertTrue(submitCallbacks.size() == 8);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));

    submitCallbacks.clear();
    pageCount = 40;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("read context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Read);
    assertTrue(submitCallbacks.size() == 8);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));

    // generate write
    submitCallbacks.clear();
    pageCount = 31;
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("write context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Write);
    assertTrue(submitCallbacks.size() == 4);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));

    submitCallbacks.clear();
    pageCount = 40;
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 0, pageCount * pageSize);
    logger.info("write context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Write);
    assertTrue(submitCallbacks.size() == 4);
    assertEquals(contexts.size(), getContextSize(submitCallbacks));
  }

  @Test
  public void testFailure() throws Exception {
    ByteBufAllocator allocator = new SimplePooledByteBufAllocator((int) cfg.getCachePoolSizeBytes(),
        cfg.getMediumPageSizeForPool(), cfg.getLittlePageSizeForPool(),
        cfg.getLargePageSizeForPool());
    ioSeparator = new IoSeparator1(
        new Storage1(true, instanceStore, clientFactory, asyncClientFactory, allocator),
        maxReadSize, maxWriteSize);
    List<IoUnitContext> contexts;
    // generate read
    failCallbacks.clear();
    int pageCount = 11;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 0, pageCount * pageSize);

    // release buffer before test
    for (IoUnitContext ioUnitContext : contexts) {
      ioUnitContext.getIoUnit().setPyBuffer(null);
    }

    IoRequest ioRequest;
    ioRequest = contexts.get(0).getIoRequest();
    logger.info("read context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Read);
    assertTrue(failCallbacks.size() == 3);
    assertTrue(ioRequest.getReferenceCount() == 0);
    assertFalse(isSuccess(contexts));

    failCallbacks.clear();
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 0, pageCount * pageSize);

    // release buffer before test
    for (IoUnitContext ioUnitContext : contexts) {
      ioUnitContext.getIoUnit().setPyBuffer(null);
    }

    ioRequest = contexts.get(0).getIoRequest();
    logger.info("write context: {}", contexts.size());
    ioSeparator.messageReceived(contexts, NbdRequestType.Write);
    assertTrue(failCallbacks.size() == 2);
    assertTrue(ioRequest.getReferenceCount() == 0);
    assertFalse(isSuccess(contexts));
  }

  private boolean isSuccess(List<IoUnitContext> contexts) {
    for (IoUnitContext ioContext : contexts) {
      if (!ioContext.getIoUnit().isSuccess()) {
        return false;
      }
    }

    return true;
  }

  private int getContextSize(List<IoUnitContextPacket> callbacks) {
    int count = 0;
    for (IoUnitContextPacket callback : callbacks) {
      count += callback.getIoContext().size();
    }
    return count;
  }

  class IoSeparator1 extends IoSeparatorImpl {

    public IoSeparator1(Coordinator coordinator, int maxNetworkFrameSizeForRead,
        int maxNetworkFrameSizeForWrite) {
      super(maxNetworkFrameSizeForRead, maxNetworkFrameSizeForWrite, coordinator);
      submitCallbacks.clear();
    }

    public void doneWithSubmitFailure(IoUnitContextPacketImpl callback) {
      failCallbacks.add(callback);
      super.doneWithSubmitFailure(callback);
    }
  }

  class Storage1 extends Coordinator {

    private boolean broken;

    public Storage1(InstanceStore instanceStore,
        GenericThriftClientFactory<DataNodeService.Iface> clientFactory,
        GenericThriftClientFactory<DataNodeService.AsyncIface> asyncClientFactory,
        ByteBufAllocator allocator) throws Exception {

      this(false, instanceStore, clientFactory, asyncClientFactory, allocator);
    }

    public Storage1(boolean broken, InstanceStore instanceStore,
        GenericThriftClientFactory<DataNodeService.Iface> clientFactory,
        GenericThriftClientFactory<DataNodeService.AsyncIface> asyncClientFactory,
        ByteBufAllocator allocator
    ) throws Exception {
      super(instanceStore, clientFactory, asyncClientFactory, allocator);
      submitCallbacks.clear();
      this.broken = broken;
    }

    @Override
    public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {

    }

    @Override
    public void read(long pos, ByteBuffer buffer) throws StorageException {

    }

    @Override
    public void write(long pos, byte[] buf, int off, int len) throws StorageException {

    }

    @Override
    public void write(long pos, ByteBuffer buffer) throws StorageException {

    }

    @Override
    public void asyncRead(long pos, byte[] dstBuf, int off, int len,
        AsyncIoCallBack asyncIoCallBack)
        throws StorageException {

    }

    @Override
    public void asyncRead(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
        throws StorageException {

    }

    @Override
    public void asyncWrite(long pos, byte[] buf, int off, int len, AsyncIoCallBack asyncIoCallBack)
        throws StorageException {

    }

    @Override
    public void asyncWrite(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
        throws StorageException {

    }

    @Override
    public long size() {
      return 0;
    }

    public void submit(IoUnitContextPacket callback) throws StorageException {
      if (broken) {
        throw new StorageException("i want to throw exception");
      }
      submitCallbacks.add(callback);
    }

    @Override
    public void close() throws StorageException {

    }

    @Override
    public void open() throws StorageException {

    }

    @Override
    public void accumulateIoRequest(Long requestUuid, IoRequest ioRequest) throws StorageException {
    }

    @Override
    public void submitIoRequests(Long requestUuid) throws StorageException {
    }
  }
}
