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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentVersion;
import py.common.DelayManager;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.lib.Coordinator;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.IoContextManager;
import py.coordinator.logmanager.IoMethodCallback;
import py.coordinator.logmanager.LogRecorder;
import py.coordinator.logmanager.UncommittedLogManager;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.logmanager.WriteMethodCallback;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.Reply;
import py.coordinator.pbrequest.RequestBuilder;
import py.coordinator.response.TriggerByCheckCallback;
import py.coordinator.task.ResendRequest;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.coordinator.worker.CommitLogWorker;
import py.icshare.BroadcastLogStatus;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.datanode.PyWriteRequest;
import py.netty.exception.TimeoutException;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.test.TestBase;
import py.volume.VolumeType;

/**
 * xx.
 */
public class WriteIoContextManagerPssaaTest extends TestBase {

  private WriteIoContextManager ioContextManager;
  private DelayManager delayResendManager = mock(DelayManager.class);
  private LogRecorder logPointerRecorder = mock(LogRecorder.class);
  private Long volumeId = RequestIdBuilder.get();
  private SegId segId = new SegId(0L, 0);

  private UncommittedLogManager uncommitLogManager = mock(UncommittedLogManager.class);
  private RequestBuilder<?> requestBuilder = mock(RequestBuilder.class);
  private CommitLogWorker commitLogWorker = mock(CommitLogWorker.class);
  private PyWriteRequest pyWriteRequest = mock(PyWriteRequest.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private SegmentMembership membership;
  private long segmentSize = 1024 * 1024;
  private int pageSize = 128;

  private IoContextGenerator ioContextGenerator;
  private AtomicLong logIdGenerator;
  private List<Reply> replies;
  private SimplePooledByteBufAllocator allocator = new SimplePooledByteBufAllocator(1024 * 1024,
      1024 * 2, 1024,
      1024 * 4);
  private int pageCount;
  private int littlePageCount;
  private AtomicLong logId = new AtomicLong(RequestIdBuilder.get());
  private Map<Long, IoContextManager> recordIoContextManager;


  @Mock
  private IoActionContext ioActionContext;


  /**
   * xx.
   */
  public WriteIoContextManagerPssaaTest() throws Exception {
    super.init();
    when(coordinator.getUncommittedLogManager()).thenReturn(uncommitLogManager);
    when(coordinator.getCommitLogWorker()).thenReturn(commitLogWorker);
    when(coordinator.getDelayManager()).thenReturn(delayResendManager);
    when(coordinator.getLogRecorder()).thenReturn(logPointerRecorder);
    when(coordinator.getVolumeType(anyLong())).thenReturn(VolumeType.REGULAR);
    this.ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
    this.logIdGenerator = new AtomicLong(0);
    this.membership = null;
    this.replies = new ArrayList<>();
    this.pageCount = allocator.getAvailableMediumPageCount();
    this.littlePageCount = allocator.getAvailableLittlePageCount();
    this.recordIoContextManager = new ConcurrentHashMap<>();
    when(coordinator.generateLogUuid()).thenReturn(logIdGenerator.incrementAndGet());
  }


  /**
   * xx.
   */
  @Before
  public void before() {
    // TODO: SegmentForm should be more forms
    SegmentMembership membership = mock(SegmentMembership.class);
    when(membership.getMemberIoStatus(any(InstanceId.class))).thenReturn(MemberIoStatus.Primary);
    when(ioActionContext.getSegmentForm()).thenReturn(SegmentForm.PSSAA);
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

  /**
   * write to three datanode and get three good response.
   */
  @Test
  public void threeGoodResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks("PSSAA", 3);

    //the before write number must == the end;
    when(ioActionContext.getTotalWriteCount()).thenReturn(3);

    // response primary
    responseGood(callbacks[0], null);

    // response secondary
    responseGood(callbacks[1], null);
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseGood(callbacks[2], null);

    verifyCreated(broadcastLogs, BroadcastLogStatus.Created);

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(0)).put(any(ResendRequest.class));
  }

  /**
   * Just SSAA  just write SS.
   **/
  @Test
  public void twoGoodResponseSsaa() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive an reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks("SSAA", 2);

    //the before write number must == the end;
    when(ioActionContext.getTotalWriteCount()).thenReturn(2);
    when(ioActionContext.isPrimaryDown()).thenReturn(true);

    // response primary
    responseGoodForSecond(callbacks[0], null);

    // response secondary
    responseGood(callbacks[1], null);

    verifyCreated(broadcastLogs, BroadcastLogStatus.Created);

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(0)).put(any(ResendRequest.class));
  }


  /**
   * write to three datanode and get two good response and a bad response.
   */
  @Test
  public void twoGoodResponseAndOneBadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.info("receive an reply");
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    ioContextGenerator.setSender(sender);
    // create write IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create logs
    IoMethodCallback[] callbacks = createCallbacks("PSSAA", 3);

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    // response primary
    responseGood(callbacks[0], null);
    // response secondary
    responseGood(callbacks[1], null);

    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    // response secondary
    responseTimeout(callbacks[2]);

    //Creating ,is the last callback if fail ,so the total write number != the get write number
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    assertEquals(replies.size(), 0);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));

    //just for the after unit
    callbacks = createCallbacks("PSSAA", 3);
    responseGood(callbacks[0], null);
    responseGood(callbacks[1], null);
    responseGood(callbacks[2], null);

  }

  /**
   * when the memory of server is exhausted, then retry two times and get good responses.
   */
  @Test
  public void resourceExhaustedResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.info("receive an reply");
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    ioContextGenerator.setSender(sender);
    // create IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = null;
    int exhaustCount = 2;

    // retry two times
    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    int retryTimes = 2;
    for (int i = 0; i < retryTimes; i++) {
      callbacks = createCallbacks(3);
      // response primary and secondary
      responseGood(callbacks[0], null);
      responseResourceExhausted(exhaustCount, callbacks[1]);
      responseResourceExhausted(exhaustCount, callbacks[2]);

      assertEquals(0, replies.size());
      verify(delayResendManager, times(i + 1)).put(any(ResendRequest.class));
    }

    // re-send and response secondary
    assertEquals(ioContextManager.getLogsToCreate().size(), exhaustCount);
    assertEquals(
        ioContextManager.getCallback().getIoContext().get(0).getIoRequest().getReferenceCount(),
        exhaustCount);

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    callbacks = createCallbacks(3);
    responseGood(callbacks[0], null);
    responseGood(callbacks[1], null);
    responseGood(callbacks[2], null);

    assertFalse(ioContextManager.isAllFinalStatus());
    commitLog(broadcastLogs, BroadcastLogStatus.Committed);
    assertEquals(replies.size(), 1);
    assertTrue(ioContextManager.isAllFinalStatus());
    verify(delayResendManager, times(retryTimes)).put(any(ResendRequest.class));
  }

  /**
   * timeout and expired finally.
   */
  @Test
  public void timeoutAndExpired() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("get reply: {}", reply);
        replies.add(reply);
        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };

    ioContextGenerator.setSender(sender);
    // create IO context manager
    List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    // create notifyAllListeners
    int retryTimes = 2;
    for (int i = 0; i < retryTimes; i++) {
      IoMethodCallback[] callbacks = createCallbacks(3);

      // get responses
      responseGood(callbacks[0], null);
      responseTimeout(callbacks[1]);
      responseTimeout(callbacks[2]);
    }
    for (BroadcastLog log : broadcastLogs) {
      assertFalse(log.isCreateSuccess());
    }

    assertEquals(0, replies.size());
    ioContextManager.setExpiredTime(System.currentTimeMillis());
    Thread.sleep(1);
    IoMethodCallback[] callbacks = createCallbacks(3);

    // get responses
    responseGood(callbacks[0], null);
    responseTimeout(callbacks[1]);
    responseTimeout(callbacks[2]);

    assertEquals(1, replies.size());
    commitLog(broadcastLogs, BroadcastLogStatus.AbortConfirmed);
    assertTrue(ioContextManager.isAllFinalStatus());
  }

  @Test
  public void threeFailResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive a reply: {}", reply);
        replies.add(reply);
      }
    };

    ioContextGenerator.setSender(sender);
    // create IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();
    membership = generateMembership();
    // fail to response and retry.

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    int retryTimes = 3;
    for (int i = 0; i < retryTimes; i++) {
      IoMethodCallback[] callbacks = createCallbacks(3);
      responseFail(callbacks[0],
          NettyExceptionHelper.buildMembershipVersionLowerException(segId, membership));
      responseFail(callbacks[1], NettyExceptionHelper
          .buildNotSecondaryException(segId, SegmentUnitStatus.PreSecondary, membership));
      responseFail(callbacks[2], NettyExceptionHelper
          .buildNotSecondaryException(segId, SegmentUnitStatus.PreSecondary, membership));
    }

    verify(delayResendManager, times(retryTimes)).put(any(ResendRequest.class));
    assertEquals(replies.size(), 0);
    for (BroadcastLog log : broadcastLogs) {
      assertFalse(log.isCreateCompletion());
    }

    ioContextManager.setExpiredTime(System.currentTimeMillis());
    Thread.sleep(1);
    IoMethodCallback[] callbacks = createCallbacks(3);
    // fail to response and expired.
    responseFail(callbacks[0],
        NettyExceptionHelper.buildMembershipVersionLowerException(segId, membership));
    responseFail(callbacks[1], NettyExceptionHelper
        .buildNotSecondaryException(segId, SegmentUnitStatus.PreSecondary, membership));
    responseFail(callbacks[2], NettyExceptionHelper
        .buildNotSecondaryException(segId, SegmentUnitStatus.PreSecondary, membership));

    // primary failure
    verify(delayResendManager, times(retryTimes)).put(any(ResendRequest.class));
    // there is no reply for the request, because the logs should be abort confirm firstly.
    assertEquals(replies.size(), 1);

    for (BroadcastLog log : broadcastLogs) {
      assertTrue(log.isCreateCompletion());
      assertFalse(log.isCreateSuccess());
    }
  }

  /**
   * xx.
   */
  @Test
  public void primaryResponseMembershipChanged() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      public void send(Reply reply) {
        logger.warn("receive a reply: {}", reply);
        replies.add(reply);
        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };

    ioContextGenerator.setSender(sender);
    membership = generateMembership();

    // create IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(3);

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);

    verify(coordinator, times(0)).updateMembershipFromDatanode(anyLong(), any(SegId.class),
        any(SegmentMembership.class), any(TriggerByCheckCallback.class));

    responseGood(callbacks[0], null);
    responseGood(callbacks[1], null);
    responseGood(callbacks[2], null);
    verify(coordinator, times(0)).updateMembershipFromDatanode(anyLong(), any(SegId.class),
        any(SegmentMembership.class), any(TriggerByCheckCallback.class));
    commitLog(broadcastLogs, BroadcastLogStatus.Committed);
    assertTrue(ioContextManager.isAllFinalStatus());

    assertEquals(replies.size(), 1);
  }

  /**
   * create logs and carry some logs to be committed successfully.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void createLogsWithCommit() throws Exception {
    List<WriteIoContextManager> managers = new ArrayList<WriteIoContextManager>();
    final List<WriteIoContextManager> existManagers = new ArrayList<WriteIoContextManager>();

    when(ioActionContext.getTotalWriteCount()).thenReturn(3);
    threeGoodResponse();
    managers.add(ioContextManager);
    existManagers.add(ioContextManager);
    assertEquals(replies.size(), 1);
    replies.clear();
    threeFailResponse();
    managers.add(ioContextManager);
    existManagers.add(ioContextManager);
    assertEquals(replies.size(), 1);

    verifyCreated(managers.get(0).getLogsToCommit(), BroadcastLogStatus.Created);
    verifyCreated(managers.get(1).getLogsToCommit(), BroadcastLogStatus.Abort);

    when(uncommitLogManager.pollLogManagerToCommit(volumeId, segId)).thenReturn(managers);

    // create write IO context manager
    final List<BroadcastLog> broadcastLogs = createWriteIoContextManager();

    // create callbacks
    IoMethodCallback[] callbacks = createCallbacks(3);

    // response primary
    responseGood(callbacks[0], managers);
    responseGood(callbacks[1], managers);
    responseTimeout(callbacks[2]);

    //One error ,so the the status Creating
    verifyCreated(broadcastLogs, BroadcastLogStatus.Creating);

    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);

    verify(uncommitLogManager, times(1)).addLogManagerToCommit(any(Collection.class));

    // create callbacks
    callbacks = createCallbacks(3);
    responseGood(callbacks[0], null);
    responseGood(callbacks[1], null);
    responseGood(callbacks[2], null);
  }

  /**
   * create logs and carry some logs to be committed unsuccessfully.
   */
  public void commitLog(Collection<BroadcastLog> logs, BroadcastLogStatus status) {
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
  public IoMethodCallback[] createCallbacks(int count) {
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
  public IoMethodCallback[] createCallbacks(String name, int count) {
    int pnumber = 0;
    int snumber = 0;
    int jnumber = 0;
    int anumber = 0;
    IoMember ioMember = null;
    String hostName = "10.0.1.";
    int iforHostName = 1;
    long forInstandId = 1L;
    boolean primaryIsDonw = false;
    AtomicInteger counter = new AtomicInteger(count);
    int callbcakNum = 0;
    int total = 0;
    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          pnumber++;
          break;
        case 'S':
          snumber++;
          break;
        case 'J':
          jnumber++;
          break;
        case 'A':
          anumber++;
          break;
        default:
          break;
      }
    }

    total = pnumber + snumber + jnumber + anumber;
    WriteMethodCallback[] callbacks = new WriteMethodCallback[total];
    if (pnumber == 1) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), iforHostName++),
          MemberIoStatus.Primary);
      callbacks[callbcakNum++] = new WriteMethodCallback(ioContextManager, counter, coordinator,
          ioMember);
    }

    for (int i = 0; i < snumber; i++) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), iforHostName++),
          MemberIoStatus.Secondary);
      callbacks[callbcakNum++] = new WriteMethodCallback(ioContextManager, counter, coordinator,
          ioMember);
    }

    for (int i = 0; i < jnumber; i++) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), iforHostName++),
          MemberIoStatus.Secondary);
      callbacks[callbcakNum++] = new WriteMethodCallback(ioContextManager, counter, coordinator,
          ioMember);
    }

    for (int i = 0; i < anumber; i++) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), iforHostName++),
          MemberIoStatus.Arbiter);
      callbacks[callbcakNum++] = new WriteMethodCallback(ioContextManager, counter, coordinator,
          ioMember);
    }

    ioContextManager.initRequestCount(count);
    return callbacks;

  }


  /**
   * xx.
   */
  public List<BroadcastLog> createWriteIoContextManager() throws Exception {
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
    ioContextManager.setExpiredTime(System.currentTimeMillis() + 100000);
    ioContextManager.setIoActionContext(ioActionContext);
    return newLogs;
  }


  /**
   * xx.
   */
  public void responseGoodForSecond(IoMethodCallback callback,
      List<WriteIoContextManager> managers) {
    List<BroadcastLog> broadcastLogs = ioContextManager.getLogsToCreate();
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList = null;
    PbIoUnitResult result = PbIoUnitResult.OK;
    if (callback.getMemberIoStatus().isSecondary()) {
      result = PbIoUnitResult.PRIMARY_COMMITTED;
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
  public void responseGood(IoMethodCallback callback, List<WriteIoContextManager> managers) {
    List<BroadcastLog> broadcastLogs = ioContextManager.getLogsToCreate();
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList = null;
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
  public void responseResourceExhausted(int exhaustCount, IoMethodCallback callback) {
    List<BroadcastLog> broadcastLogs = ioContextManager.getLogsToCreate();
    // keep keys in map in order
    LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList = null;
    ioContextToResultPairList = generateUnitResults(broadcastLogs.subList(0, exhaustCount),
        PbIoUnitResult.EXHAUSTED);
    if (broadcastLogs.size() > exhaustCount) {
      List<BroadcastLog> successLogs = broadcastLogs.subList(exhaustCount, broadcastLogs.size());
      ioContextToResultPairList.addAll(generateUnitResults(successLogs, PbIoUnitResult.OK));
    }
    PbWriteResponse response = NbdRequestResponseGenerator
        .generateWriteResponse(RequestIdBuilder.get(), ioContextToResultPairList, null);
    callback.complete(response);
  }

  public void responseTimeout(IoMethodCallback callback) {
    callback.fail(new TimeoutException());
  }

  public void responseFail(IoMethodCallback callback, Exception exception) {
    callback.fail(exception);
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

  private SegmentMembership generateMembership() {
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(RequestIdBuilder.get()));
    secondaries.add(new InstanceId(RequestIdBuilder.get()));
    return new SegmentMembership(new SegmentVersion(0, 1), new InstanceId(RequestIdBuilder.get()),
        secondaries);
  }
}
