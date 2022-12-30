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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.buffer.PyBuffer;
import py.common.DelayManager;
import py.common.RequestIdBuilder;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounit.ReadIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.SkipForSourceVolumeIoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.iorequest.iounitcontextpacket.SkipForSourceVolumeIoUnitContextPacket;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.log.BroadcastLogForLinkedCloneVolume;
import py.coordinator.logmanager.ClonedSourceVolumeListenerForWriteIO;
import py.coordinator.logmanager.IoContextManager;
import py.coordinator.logmanager.LogRecorder;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.UncommittedLogManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.pbrequest.RequestBuilder;
import py.coordinator.task.ResendRequest;
import py.coordinator.utils.DummyNetworkDelayRecorder;
import py.coordinator.utils.NetworkDelayRecorder;
import py.coordinator.worker.CommitLogWorker;
import py.membership.IoActionContext;
import py.membership.SegmentMembership;
import py.netty.datanode.PyWriteRequest;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.test.TestBase;
import py.volume.VolumeType;

/**
 * xx.
 */
public class ClonedSourceVolumeListenerForWriteIoTest extends TestBase {

  private ClonedSourceVolumeListenerForWriteIO sourceVolumeListenerForWriteIo;
  private Long volumeId = RequestIdBuilder.get();
  private SegId segId = new SegId(volumeId, 0);
  private WriteIoContextManager ioContextManager;
  private ReadIoContextManager readIoContextManager;
  @Mock
  private IoActionContext ioActionContext;

  private DelayManager delayResendManager = mock(DelayManager.class);
  private LogRecorder logPointerRecorder = mock(LogRecorder.class);
  private UncommittedLogManager uncommitLogManager = mock(UncommittedLogManager.class);
  private PyWriteRequest pyWriteRequest = mock(PyWriteRequest.class);
  private RequestBuilder<?> requestBuilder = mock(RequestBuilder.class);
  private CommitLogWorker commitLogWorker = mock(CommitLogWorker.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private SegmentMembership membership;

  private NetworkDelayRecorder networkDelayRecorder = new DummyNetworkDelayRecorder();
  private AtomicLong logIdGenerator = new AtomicLong(0);
  private long segmentSize = 1024 * 1024;
  private int pageSize = 128;

  private IoContextGenerator ioContextGenerator;
  private SimplePooledByteBufAllocator allocator = new LoggedSimpleByteBufferAllocator(1024 * 1024,
      1024 * 2, 1024, 1024 * 4);


  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    when(coordinator.getUncommittedLogManager()).thenReturn(uncommitLogManager);
    when(coordinator.getCommitLogWorker()).thenReturn(commitLogWorker);
    when(coordinator.getDelayManager()).thenReturn(delayResendManager);
    when(coordinator.getLogRecorder()).thenReturn(logPointerRecorder);
    when(coordinator.getNetworkDelayRecorder()).thenReturn(networkDelayRecorder);
    when(coordinator.getVolumeType(anyLong())).thenReturn(VolumeType.REGULAR);
    this.ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
  }

  @Test
  public void readAndWriteDataMergeTest() throws Exception {
    List<BroadcastLog> logs = createWriteIoContextManager();
    Map<IoUnitContext, BroadcastLog> logsMap = new HashMap<>();
    List<IoUnitContext> ioUnitContexts = new ArrayList<>();
    Map<IoUnitContext, BroadcastLog> waitReadSourceVolumeLogs = new HashMap<>();
    List<byte[]> originData = new ArrayList<>();
    List<byte[]> destData = new ArrayList<>();
    for (BroadcastLog broadcastLog : logs) {
      int pageSize = this.pageSize;
      long offset = (broadcastLog.getOffset() / pageSize) * pageSize;
      IoUnit ioUnit = new ReadIoUnitImpl(segId.getIndex(),
          broadcastLog.getPageIndexInSegment(), offset, pageSize);
      byte[] data = new byte[pageSize];
      ByteBuf byteBuf = Unpooled.wrappedBuffer(data);
      byteBuf.clear();
      for (int i = 0; i < pageSize / 8; i++) {
        byteBuf.writeLong(offset + i);
      }
      ioUnit.setPyBuffer(new PyBuffer(byteBuf));
      ioUnit.setSuccess(true);
      destData.add(data);

      IoUnitContext ioUnitContext = new SkipForSourceVolumeIoUnitContext(
          IoRequestType.Read, ioUnit, segId, broadcastLog.getPageIndexInSegment(), null);
      ioUnitContexts.add(ioUnitContext);

      waitReadSourceVolumeLogs.put(ioUnitContext, broadcastLog);

      byte[] origin = new byte[broadcastLog.getIoContext().getIoUnit().getLength()];
      ByteBuf originBuf = broadcastLog.getIoContext().getIoUnit().getPyBuffer().getByteBuf()
          .duplicate();
      originBuf.getBytes(originBuf.readerIndex(), origin);
      originData.add(origin);
    }

    sourceVolumeListenerForWriteIo = new ClonedSourceVolumeListenerForWriteIO(
        ioContextManager, waitReadSourceVolumeLogs);

    IoUnitContextPacket ioUnitContextPacket = new SkipForSourceVolumeIoUnitContextPacket(
        volumeId, 0, ioUnitContexts, segId.getIndex(), IoRequestType.Read,
            sourceVolumeListenerForWriteIo);

    ReadIoContextManager ioContextManager = new ReadIoContextManager(volumeId,
        new SegId(volumeId, segId.getIndex()), ioUnitContextPacket, coordinator);
    sourceVolumeListenerForWriteIo.setReadIoContextManager(ioContextManager);

    final List<IoContextManager> resendWriteContext = new ArrayList<>();
    doAnswer(v -> {
      resendWriteContext.add(((ResendRequest) v.getArgument(0)).getIoContextManager());
      return v;
    }).when(delayResendManager).put(any(ResendRequest.class));
    sourceVolumeListenerForWriteIo.done();

    assertEquals(1, resendWriteContext.size());

    List<BroadcastLog> broadcastLogs = ((WriteIoContextManager) resendWriteContext.get(0))
        .getLogsToCreate();

    assertTrue(broadcastLogs.size() > 0);
    assertTrue(broadcastLogs.get(0) instanceof BroadcastLogForLinkedCloneVolume);

    for (int i = 0; i < broadcastLogs.size(); i++) {
      assertTrue(broadcastLogs.get(i) instanceof BroadcastLogForLinkedCloneVolume);

      BroadcastLogForLinkedCloneVolume log = (BroadcastLogForLinkedCloneVolume) broadcastLogs
          .get(0);

      assertEquals(log.getSourceDataLength(), logs.get(i).getLength());
      assertEquals(log.getSourceDataOffset(), logs.get(i).getOffset());

      assertEquals(pageSize, log.getLength());
      long pageOffset = (log.getOffset() / pageSize) * pageSize;
      assertEquals(pageOffset, log.getOffset());

      byte[] bytes = new byte[log.getSourceDataLength()];
      int diffOffset = (int) (log.getSourceDataOffset() - log.getOffset());
      log.getPyBuffer().getByteBuf().duplicate()
          .getBytes(log.getPyBuffer().getByteBuf().readerIndex() + diffOffset, bytes, 0,
              log.getSourceDataLength());

      assertArrayEquals(originData.get(i), bytes);

      byte[] data = destData.get(i);
      for (int j = diffOffset; j < log.getSourceDataLength(); j++) {
        data[j] = bytes[j - diffOffset];
      }

      byte[] page = new byte[log.getLength()];
      log.getPyBuffer().getByteBuf().duplicate()
          .getBytes(log.getPyBuffer().getByteBuf().readerIndex(), page, 0,
              log.getLength());

      assertArrayEquals(data, page);
    }
  }


  /**
   * xx.
   */
  public List<BroadcastLog> createWriteIoContextManager() throws Exception {
    byte[] data = NbdRequestResponseGenerator.getBuffer(pageSize / 2, 1);
    byte[] tmpRequest = NbdRequestResponseGenerator
        .generateWriteRequestPlan(127, pageSize / 2, data);
    ByteBuf request = allocator.buffer(tmpRequest.length);
    request.writeBytes(tmpRequest);

    List<IoUnitContext> contexts = ioContextGenerator.generateWriteIoContexts(volumeId, request);
    assertEquals(contexts.size(), 1);

    IoUnitContextPacket ioUnitContextPacket = new IoUnitContextPacketImpl(volumeId, contexts, 0,
        contexts.get(0).getRequestType());
    List<BroadcastLog> newLogs = createLogs(ioUnitContextPacket.getIoContext());
    assertEquals(newLogs.size(), 1);
    ioContextManager = new WriteIoContextManager(volumeId, segId, ioUnitContextPacket, coordinator,
        newLogs, IoRequestType.Write);
    ioContextManager.setRequestBuilder(requestBuilder);
    ioContextManager.setExpiredTime(System.currentTimeMillis() + 100000);
    ioContextManager.setIoActionContext(ioActionContext);
    return newLogs;
  }


  /**
   * xx.
   */
  public List<BroadcastLog> createLogs(Collection<IoUnitContext> contexts) {
    List<BroadcastLog> logs = new ArrayList<BroadcastLog>();
    for (IoUnitContext ioContext : contexts) {
      BroadcastLog log = new BroadcastLog(logIdGenerator.incrementAndGet(), ioContext, 0);
      logs.add(log);
    }

    return logs;
  }

  private List<IoUnitContext> createReadIoContextManager(long volumeId, long offset, int length)
      throws Exception {
    List<IoUnitContext> contexts = ioContextGenerator
        .generateReadIoContexts(volumeId, offset, length);
    IoUnitContextPacket ioUnitContextPacket = new IoUnitContextPacketImpl(volumeId, contexts, 0,
        IoRequestType.Read);
    readIoContextManager = new ReadIoContextManager(volumeId, segId, ioUnitContextPacket,
        coordinator);
    readIoContextManager.setExpiredTime(System.currentTimeMillis() + 10000000);
    readIoContextManager.setIoActionContext(ioActionContext);
    return contexts;
  }

  private class LoggedSimpleByteBufferAllocator extends SimplePooledByteBufAllocator {

    public LoggedSimpleByteBufferAllocator(int poolSize, int pageMediumSize, int littlePageSize,
        int largePageSize) {
      super(poolSize, pageMediumSize, littlePageSize, largePageSize, "network");
    }

    @Override
    public void release(ByteBuf byteBuf) {
      super.release(byteBuf);
    }
  }
}
