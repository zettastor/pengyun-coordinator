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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.DelayManager;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.IoContextManager;
import py.coordinator.logmanager.IoMethodCallback;
import py.coordinator.logmanager.LogRecorder;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.ReadMethodCallback;
import py.coordinator.logmanager.UncommittedLogManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.logmanager.WriteMethodCallback;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.Reply;
import py.coordinator.pbrequest.RequestBuilder;
import py.coordinator.task.ResendRequest;
import py.coordinator.utils.DummyNetworkDelayRecorder;
import py.coordinator.utils.NetworkDelayRecorder;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.coordinator.worker.CommitLogWorker;
import py.icshare.BroadcastLogStatus;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.netty.datanode.PyReadResponse;
import py.netty.datanode.PyWriteRequest;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.ReadCause;
import py.test.TestBase;
import py.volume.VolumeType;

/**
 * xx.
 */
public class WriteIoContextManagerLinkedCloneTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(WriteIoContextManagerTest.class);
  private WriteIoContextManager ioContextManager;
  private DelayManager delayResendManager = mock(DelayManager.class);
  private LogRecorder logPointerRecorder = mock(LogRecorder.class);
  private Long volumeId = RequestIdBuilder.get();
  private long moveOnlineVolumeId = RequestIdBuilder.get();
  private SegId segId = new SegId(volumeId, 0);

  private UncommittedLogManager uncommitLogManager = mock(UncommittedLogManager.class);
  private PyWriteRequest pyWriteRequest = mock(PyWriteRequest.class);
  private RequestBuilder<?> requestBuilder = mock(RequestBuilder.class);
  private CommitLogWorker commitLogWorker = mock(CommitLogWorker.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private SegmentMembership membership;
  private long segmentSize = 1024 * 1024;
  private int pageSize = 128;
  private NetworkDelayRecorder networkDelayRecorder = new DummyNetworkDelayRecorder();

  private IoContextGenerator ioContextGenerator;
  private AtomicLong logIdGenerator;
  private List<Reply> replies;
  private SimplePooledByteBufAllocator allocator = new LoggedSimpleByteBufferAllocator(1024 * 1024,
      1024 * 2, 1024, 1024 * 4);
  private int pageCount;
  private int littlePageCount;
  private AtomicLong logId = new AtomicLong(RequestIdBuilder.get());
  private Map<Long, IoContextManager> recordIoContextManager;
  private BlockingQueue<ResendRequest> resendQueue = new LinkedBlockingQueue<>();
  private SpaceSavingVolumeMetadata volumeMetadata = mock(SpaceSavingVolumeMetadata.class);
  @Mock
  private IoActionContext ioActionContext;


  /**
   * xx.
   */
  public WriteIoContextManagerLinkedCloneTest() throws Exception {
    super.init();
    when(coordinator.getUncommittedLogManager()).thenReturn(uncommitLogManager);
    when(coordinator.getCommitLogWorker()).thenReturn(commitLogWorker);
    when(coordinator.getDelayManager()).thenReturn(delayResendManager);

    when(coordinator.getLogRecorder()).thenReturn(logPointerRecorder);
    when(coordinator.getNetworkDelayRecorder()).thenReturn(networkDelayRecorder);
    when(coordinator.getVolumeType(anyLong())).thenReturn(VolumeType.REGULAR);
    when(coordinator.getVolumeMetaData(anyLong())).thenReturn(volumeMetadata);
    when(volumeMetadata.getSourceVolumeId()).thenReturn(1L);
    when(volumeMetadata.getSourceSnapshotId()).thenReturn(1);
    this.ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
    this.logIdGenerator = new AtomicLong(0);
    this.membership = null;
    this.replies = new ArrayList<>();
    this.pageCount = allocator.getAvailableMediumPageCount();
    this.littlePageCount = allocator.getAvailableLittlePageCount();
    this.recordIoContextManager = new ConcurrentHashMap<>();
    when(coordinator.generateLogUuid()).thenReturn(logIdGenerator.incrementAndGet());

    CoordinatorConfigSingleton.getInstance().setSegmentSize(segmentSize);
    CoordinatorConfigSingleton.getInstance().setPageSize(pageSize);
  }


  /**
   * xx.
   */
  @Before
  public void before() {
    // TODO: SegmentForm should be more forms
    SegmentMembership membership = mock(SegmentMembership.class);
    when(membership.getMemberIoStatus(any(InstanceId.class))).thenReturn(MemberIoStatus.Primary);
    when(ioActionContext.getSegmentForm()).thenReturn(SegmentForm.PSS);
    when(ioActionContext.getMembershipWhenIoCome()).thenReturn(membership);
    when(coordinator.getRequestVolumeId()).thenReturn(new AtomicLong(volumeId));
    this.replies.clear();
    this.membership = null;
    when((PyWriteRequest) requestBuilder.getRequest()).thenReturn(pyWriteRequest);
  }

  @After
  public void after() {
    assertEquals(littlePageCount, allocator.getAvailableLittlePageCount());
    assertEquals(pageCount, allocator.getAvailableMediumPageCount());
  }

  @Test
  public void by() {
    byte[] data = new byte[64];
    for (int i = 0; i < 64; i++) {
      data[i] = (byte) (i / 8);
    }
    ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

    byte[] another = new byte[64];

    ByteBuf anotherBuf = Unpooled.wrappedBuffer(another);
    logger.error("{}", anotherBuf.readableBytes());
    ByteBuf shadowBuf = anotherBuf.duplicate();
    byteBuf.getBytes(32, shadowBuf, 32, 32);

    logger.error("{}", anotherBuf.readableBytes());
    anotherBuf.clear();
    logger.error("{}", anotherBuf.readableBytes());
  }

  /**
   * write to three datanode and get three good response.
   */
  @Test
  public void threeGoodSourceResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        IoActionContext ioActionContext = new IoActionContext();
        ioActionContext.setMembershipWhenIoCome(membership);
        ioActionContext.setSegmentForm(SegmentForm.PSS);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseReadGood(readIoContextManager, mock(EndPoint.class), 3);
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(ioContextManager, 3);

    // response primary
    responseFree(callbacks[0], null);

    // response secondary
    responseFree(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseFree(callbacks[2], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    for (ResendRequest request : resendRequests) {
      request.getIoContextManager().setIoActionContext(ioActionContext);
      WriteIoContextManager writeIoContextManager = (WriteIoContextManager) request
          .getIoContextManager();
      IoMethodCallback[] callback = createCallbacks(
          writeIoContextManager, 3);
      responseGood(writeIoContextManager, callback[0], null);
      responseGood(writeIoContextManager, callback[1], null);
      responseGood(writeIoContextManager, callback[2], null);
    }
    Utils.waitUntilConditionMatches(4, () -> {
      return broadcastLogs.stream().allMatch(broadcastLog -> {
        return broadcastLog.getStatus() == BroadcastLogStatus.Created;
      });
    });

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(2)).put(any(ResendRequest.class));
  }

  @Test
  public void resendWriteTwoGoodResponseAndOneBadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        IoActionContext ioActionContext = new IoActionContext();
        ioActionContext.setMembershipWhenIoCome(membership);
        ioActionContext.setSegmentForm(SegmentForm.PSS);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseReadGood(readIoContextManager, mock(EndPoint.class), 3);
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(ioContextManager, 3);

    // response primary
    responseFree(callbacks[0], null);

    // response secondary
    responseFree(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseTimeout(callbacks[2]);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    for (ResendRequest request : resendRequests) {
      request.getIoContextManager().setIoActionContext(ioActionContext);
      WriteIoContextManager writeIoContextManager = (WriteIoContextManager) request
          .getIoContextManager();
      callbacks = createCallbacks(
          writeIoContextManager, 3);
      responseGood(writeIoContextManager, callbacks[0], null);
      responseGood(writeIoContextManager, callbacks[1], null);
      responseTimeout(callbacks[2]);
    }
    Utils.waitUntilConditionMatches(4, () -> {
      return broadcastLogs.stream().allMatch(broadcastLog -> {
        return broadcastLog.getStatus() == BroadcastLogStatus.Created;
      });
    });

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(2)).put(any(ResendRequest.class));
  }

  @Test
  public void twoGoodResponseAndOneBadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        IoActionContext ioActionContext = new IoActionContext();
        ioActionContext.setMembershipWhenIoCome(membership);
        ioActionContext.setSegmentForm(SegmentForm.PSS);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseReadGood(readIoContextManager, mock(EndPoint.class), 3);
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(ioContextManager, 3);

    // response primary
    responseFree(callbacks[0], null);

    // response secondary
    responseFree(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseTimeout(callbacks[2]);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    for (ResendRequest request : resendRequests) {
      request.getIoContextManager().setIoActionContext(ioActionContext);
      WriteIoContextManager writeIoContextManager = (WriteIoContextManager) request
          .getIoContextManager();
      IoMethodCallback[] callback = createCallbacks(
          writeIoContextManager, 3);
      responseGood(writeIoContextManager, callback[0], null);
      responseGood(writeIoContextManager, callback[1], null);
      responseGood(writeIoContextManager, callback[2], null);
    }
    Utils.waitUntilConditionMatches(4, () -> {
      return broadcastLogs.stream().allMatch(broadcastLog -> {
        return broadcastLog.getStatus() == BroadcastLogStatus.Created;
      });
    });

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(2)).put(any(ResendRequest.class));
  }

  @Test
  public void resendWriteOneGoodResponseAndTwoBadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        IoActionContext ioActionContext = new IoActionContext();
        ioActionContext.setMembershipWhenIoCome(membership);
        ioActionContext.setSegmentForm(SegmentForm.PSS);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseReadGood(readIoContextManager, mock(EndPoint.class), 3);
      } else {
        logger.debug("add a resend request to resend queue");
        resendQueue.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(ioContextManager, 3);

    // response primary
    responseFree(callbacks[0], null);

    // response secondary
    responseFree(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseTimeout(callbacks[2]);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    ResendRequest request = null;
    int times = 1;
    while (null != (request = resendQueue.take())) {
      request.getIoContextManager().setIoActionContext(ioActionContext);
      WriteIoContextManager writeIoContextManager = (WriteIoContextManager) request
          .getIoContextManager();
      callbacks = createCallbacks(
          writeIoContextManager, 3);
      responseGood(writeIoContextManager, callbacks[0], null);
      responseTimeout(callbacks[1]);
      responseTimeout(callbacks[2]);
      logger.debug("one timeout result has been processed");
      if (--times == 0) {
        break;
      }
    }
    commitLog(ioContextManager, broadcastLogs, BroadcastLogStatus.AbortConfirmed);
    Utils.waitUntilConditionMatches(4, () -> {
      return broadcastLogs.stream().allMatch(broadcastLog -> {
        return broadcastLog.getStatus() == BroadcastLogStatus.AbortConfirmed;
      });
    });

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(2)).put(any(ResendRequest.class));
  }

  @Test
  public void resendReadSourceVolumeFailed() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        final ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        Thread.sleep(300);
        IoActionContext ioActionContext = new IoActionContext();
        ioActionContext.setMembershipWhenIoCome(membership);
        ioActionContext.setSegmentForm(SegmentForm.PSS);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseReadTimeout(readIoContextManager, mock(EndPoint.class), 3);
      } else {
        logger.debug("add a resend request to resend queue");
        resendQueue.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(ioContextManager, 3);

    // response primary
    responseFree(callbacks[0], null);

    // response secondary
    responseFree(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseTimeout(callbacks[2]);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Abort);

    commitLog(ioContextManager, broadcastLogs, BroadcastLogStatus.AbortConfirmed);
    Utils.waitUntilConditionMatches(4, () -> {
      return broadcastLogs.stream().allMatch(broadcastLog -> {
        return broadcastLog.getStatus() == BroadcastLogStatus.AbortConfirmed;
      });
    });

    assertEquals(replies.size(), 1);
  }

  private List<BroadcastLog> createWriteIoContextManager() throws Exception {
    byte[] data = NbdRequestResponseGenerator.getBuffer(4 * pageSize, 1);
    byte[] tmpRequest = NbdRequestResponseGenerator
        .generateWriteRequestPlan(127, 4 * pageSize, data);
    ByteBuf request = allocator.buffer(tmpRequest.length);
    request.writeBytes(tmpRequest);

    List<IoUnitContext> contexts = ioContextGenerator.generateWriteIoContexts(volumeId, request);
    assertEquals(contexts.size(), 5);

    IoUnitContextPacket ioUnitContextPacket = new IoUnitContextPacketImpl(volumeId, contexts, 0,
        contexts.get(0).getRequestType());
    List<BroadcastLog> newLogs = createLogs(ioUnitContextPacket.getIoContext());
    assertEquals(newLogs.size(), 5);
    ioContextManager = new WriteIoContextManager(volumeId, segId, ioUnitContextPacket, coordinator,
        newLogs, IoRequestType.Write);
    ioContextManager.setRequestBuilder(requestBuilder);
    ioContextManager.setExpiredTime(System.currentTimeMillis() + 10000);
    ioContextManager.setIoActionContext(ioActionContext);
    return newLogs;
  }

  private List<BroadcastLog> createLogs(Collection<IoUnitContext> contexts) {
    List<BroadcastLog> logs = new ArrayList<BroadcastLog>();
    for (IoUnitContext ioContext : contexts) {
      BroadcastLog log = new BroadcastLog(logIdGenerator.incrementAndGet(), ioContext, 0);
      logs.add(log);
    }

    return logs;
  }


  /**
   * xx.
   */
  public IoMethodCallback createCallback(WriteIoContextManager ioContextManager, int index,
      AtomicInteger count) {
    int i = index;
    WriteMethodCallback callback = null;
    String hostName = "10.0.1." + String.valueOf(i);
    IoMember ioMember = new IoMember(new InstanceId((long) i), new EndPoint(hostName, i),
        (i == 0) ? MemberIoStatus.Primary : MemberIoStatus.Secondary);
    ioContextManager.setIoActionContext(ioActionContext);
    callback = new WriteMethodCallback(ioContextManager, count, coordinator, ioMember);
    ioContextManager.initRequestCount(3);
    return callback;
  }

  public void responseTimeout(IoMethodCallback callback) {
    callback.fail(new TimeoutException());
  }

  private void responseReadSecondaryGood(ReadIoContextManager ioContextManager, EndPoint endPoint) {
    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Secondary,
        ReadCause.CHECK);
    ioContextManager.getIoActionContext().addIoMember(ioMember);
    // TODO: send count should change
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, new AtomicInteger(1),
        coordinator,
        ioMember);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[1];
    callbacks[0] = callback;
    ioContextManager.initRequestCount(callbacks.length);
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = generateContext(ioContextManager.getRequestUnits(),
        PbIoUnitResult.OK, false);
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }

  private void responseReadPrimaryGood(ReadIoContextManager ioContextManager, EndPoint endPoint) {
    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Primary,
        Broadcastlog.ReadCause.FETCH);
    ioContextManager.getIoActionContext().addIoMember(ioMember);
    // TODO: send count should change
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, new AtomicInteger(1),
        coordinator,
        ioMember);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[1];
    callbacks[0] = callback;
    ioContextManager.initRequestCount(callbacks.length);
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = generateContext(ioContextManager.getRequestUnits(),
        PbIoUnitResult.OK, true);
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }

  private void responseReadGood(ReadIoContextManager ioContextManager, EndPoint endPoint,
      int count) {
    ReadMethodCallback[] callbacks = new ReadMethodCallback[count];
    AtomicInteger atomicInteger = new AtomicInteger(count);
    ioContextManager.initRequestCount(callbacks.length);
    for (int i = 0; i < count; i++) {
      IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
          i == 0 ? MemberIoStatus.Primary : MemberIoStatus.Secondary,
          i == 0 ? Broadcastlog.ReadCause.FETCH : ReadCause.CHECK);
      ioContextManager.getIoActionContext().addIoMember(ioMember);
      // TODO: send count should change
      ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, atomicInteger,
          coordinator,
          ioMember);
      callbacks[0] = callback;
      Pair<PbReadResponseUnit, ByteBuf>[] pairs = generateContext(
          ioContextManager.getRequestUnits(),
          PbIoUnitResult.OK, 0 == i);
      PyReadResponse response = NbdRequestResponseGenerator
          .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
      callback.complete(response);
    }
  }

  private void responseReadTimeout(ReadIoContextManager ioContextManager, EndPoint endPoint,
      int count) {
    ReadMethodCallback[] callbacks = new ReadMethodCallback[count];
    AtomicInteger atomicInteger = new AtomicInteger(count);
    ioContextManager.initRequestCount(callbacks.length);
    for (int i = 0; i < count; i++) {
      IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
          i == 0 ? MemberIoStatus.Primary : MemberIoStatus.Secondary,
          i == 0 ? Broadcastlog.ReadCause.FETCH : ReadCause.CHECK);
      ioContextManager.getIoActionContext().addIoMember(ioMember);
      // TODO: send count should change
      ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, atomicInteger,
          coordinator,
          ioMember);
      callback.fail(new TimeoutException());
    }
  }


  /**
   * xx.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Pair<PbReadResponseUnit, ByteBuf>[] generateContext(List<IoUnitContext> contexts,
      PbIoUnitResult result, boolean isPrimary) {
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = new Pair[contexts.size()];
    for (int i = 0; i < contexts.size(); i++) {
      IoUnit ioUnit = contexts.get(i).getIoUnit();
      PbReadRequestUnit unit = PbRequestResponsePbHelper.buildPbReadRequestUnitFrom(ioUnit);
      if (result == PbIoUnitResult.OK) {

        ByteBuf buf;
        if (isPrimary) {
          byte[] data = NbdRequestResponseGenerator.getBuffer(ioUnit.getLength(), 1);
          buf = allocator.buffer(ioUnit.getLength());
          buf.writeBytes(data);
        } else {
          buf = new EmptyByteBuf(allocator);
        }

        PbReadResponseUnit responseUnit = PbRequestResponseHelper
            .buildPbReadResponseUnitFrom(unit, buf);
        pairs[i] = new Pair(responseUnit, buf);
      } else {
        PbReadResponseUnit responseUnit = PbRequestResponseHelper
            .buildPbReadResponseUnitFrom(unit, result);
        pairs[i] = new Pair(responseUnit, null);
      }
    }

    return pairs;
  }


  /**
   * xx.
   */
  public IoMethodCallback[] createCallbacks(WriteIoContextManager ioContextManager, int count) {
    WriteMethodCallback[] callbacks = new WriteMethodCallback[count];
    AtomicInteger counter = new AtomicInteger(count);
    for (int i = 0; i < count; i++) {
      String hostName = "10.0.1." + String.valueOf(i);
      IoMember ioMember = new IoMember(new InstanceId((long) i), new EndPoint(hostName, i),
          (i == 0) ? MemberIoStatus.Primary : MemberIoStatus.Secondary);
      callbacks[i] = new WriteMethodCallback(ioContextManager, counter, coordinator, ioMember);
    }
    ioContextManager.initRequestCount(callbacks.length);
    return callbacks;
  }


  /**
   * xx.
   */
  public void responseFree(IoMethodCallback callback, List<WriteIoContextManager> managers) {
    List<BroadcastLog> broadcastLogs = ioContextManager.getLogsToCreate();
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList;
    PbIoUnitResult result;
    result = PbIoUnitResult.FREE;
    ioContextToResultPairList = generateUnitResults(broadcastLogs, result);
    PbWriteResponse response = NbdRequestResponseGenerator
        .generateWriteResponse(RequestIdBuilder.get(),
            ioContextToResultPairList, managers,
            callback.getMemberIoStatus().isPrimary() ? membership : null);
    callback.complete(response);
  }


  /**
   * xx.
   */
  public void responseGood(WriteIoContextManager ioContextManager, IoMethodCallback callback,
      List<WriteIoContextManager> managers) {
    List<BroadcastLog> broadcastLogs = ioContextManager.getLogsToCreate();
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList;
    PbIoUnitResult result;
    if (callback.getMemberIoStatus().isPrimary()) {
      result = PbIoUnitResult.PRIMARY_COMMITTED;
    } else {
      result = PbIoUnitResult.OK;
    }
    ioContextToResultPairList = generateUnitResults(broadcastLogs, result);
    PbWriteResponse response = NbdRequestResponseGenerator
        .generateWriteResponse(RequestIdBuilder.get(),
            ioContextToResultPairList, managers,
            callback.getMemberIoStatus().isPrimary() ? membership : null);
    callback.complete(response);
  }


  /**
   * xx.
   */
  public void verifyCreated(Collection<BroadcastLog> logs, BroadcastLogStatus status) {
    for (BroadcastLog log : logs) {
      assertEquals(log.getStatus(), status);
    }
  }


  /**
   * xx.
   */
  public LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> generateUnitResults(
      List<BroadcastLog> contexts,
      PbIoUnitResult result) {
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList =
        new LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>>();

    for (BroadcastLog ioContext : contexts) {
      // simulate primary generate log ID
      if (ioContext.getLogId() == 0) {
        ioContext.setLogId(logId.incrementAndGet());
      }
      assertTrue(!ioContext.isCreateCompletion());
      PbWriteRequestUnit unit = PbRequestResponsePbHelper.buildPbWriteRequestUnitFrom(ioContext);
      Pair<PbWriteRequestUnit, PbIoUnitResult> pair = new Pair<PbWriteRequestUnit, PbIoUnitResult>(
          unit, result);
      ioContextToResultPairList.add(pair);
    }

    return ioContextToResultPairList;
  }


  /**
   * xx.
   */
  public void commitLog(WriteIoContextManager ioContextManager, Collection<BroadcastLog> logs,
      BroadcastLogStatus status) {
    List<PbBroadcastLog> pbLogs = new ArrayList<PbBroadcastLog>();

    for (BroadcastLog log : logs) {
      pbLogs.add(PbRequestResponsePbHelper.buildPbBroadcastLogFrom(log).toBuilder()
          .setLogStatus(status.getPbLogStatus()).build());
    }

    int index = 0;
    for (PbBroadcastLog log : pbLogs) {
      ioContextManager.commitPbLog(index++, log);
    }
  }

  private class LoggedSimpleByteBufferAllocator extends SimplePooledByteBufAllocator {

    public LoggedSimpleByteBufferAllocator(int poolSize, int pageMediumSize, int littlePageSize,
        int largePageSize) {
      super(poolSize, pageMediumSize, littlePageSize, largePageSize, "network");
    }

    @Override
    public void release(ByteBuf byteBuf) {
      logger.debug("got byte buffer release", new Exception());
      super.release(byteBuf);
    }
  }
}
