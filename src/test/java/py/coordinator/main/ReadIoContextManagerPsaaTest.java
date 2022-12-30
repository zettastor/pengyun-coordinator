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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.DelayManager;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacket;
import py.coordinator.iorequest.iounitcontextpacket.IoUnitContextPacketImpl;
import py.coordinator.lib.Coordinator;
import py.coordinator.logmanager.IoMethodCallback;
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.ReadMethodCallback;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.Reply;
import py.coordinator.task.ResendRequest;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.netty.datanode.PyReadResponse;
import py.netty.exception.ServerProcessException;
import py.netty.exception.TimeoutException;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.test.TestBase;
import py.volume.VolumeType;

@Ignore
public class ReadIoContextManagerPsaaTest extends TestBase {

  private ReadIoContextManager ioContextManager;
  private SimplePooledByteBufAllocator allocator = new SimplePooledByteBufAllocator(
      16 * 1024 * 1024, 4 * 1024);
  private DelayManager delayResendManager = mock(DelayManager.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private EndPoint endPoint = mock(EndPoint.class);

  private Long volumeId = RequestIdBuilder.get();
  private SegId segId = mock(SegId.class);
  private IoContextGenerator ioContextGenerator;

  private long segmentSize = 1024 * 1024;
  private int pageSize = 128;
  private int pageCount;
  private int littlePageCount;
  private List<Reply> replies = new ArrayList<Reply>();
  private IoActionContext ioActionContext = mock(IoActionContext.class);
  private SegmentMembership membership = mock(SegmentMembership.class);

  private Set<IoMember> checkReaders;
  private Set<IoMember> mergeReaders;
  private Set<IoMember> fetchReaders;


  /**
   * xx.
   */
  public ReadIoContextManagerPsaaTest() throws Exception {
    super.init();
    checkReaders = new HashSet<>();
    mergeReaders = new HashSet<>();
    fetchReaders = new HashSet<>();
    ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
    when(coordinator.getDelayManager()).thenReturn(delayResendManager);
    when(coordinator.getVolumeType(anyLong())).thenReturn(VolumeType.LARGE);
    pageCount = allocator.getAvailableMediumPageCount();
    littlePageCount = allocator.getAvailableLittlePageCount();
  }


  /**
   * xx.
   */
  @Before
  public void beforeMethod() {
    // TODO: SegmentForm.PJ
    when(ioActionContext.getSegmentForm()).thenReturn(SegmentForm.PSSAA);
    Set<IoMember> ioMembers = new HashSet<>();
    ioMembers
        .add(new IoMember(new InstanceId(RequestIdBuilder.get()), new EndPoint("127.0.0.1", 6789),
            MemberIoStatus.Primary, Broadcastlog.ReadCause.FETCH));
    when(ioActionContext.getIoMembers()).thenReturn(ioMembers);
    when(ioActionContext.getRealReaders()).thenReturn(ioMembers);
    when(ioActionContext.getFetchReader()).thenReturn(ioMembers);
    when(ioActionContext.getCheckReaders()).thenReturn(checkReaders);
    when(ioActionContext.getMembershipWhenIoCome()).thenReturn(membership);
    replies.clear();
  }

  @After
  public void afterMethod() {
    assertEquals(pageCount, allocator.getAvailableMediumPageCount());
    //        assertEquals(littlePageCount, allocator.getAvailableLittlePageCount());
  }

  private void addMergeReaders(IoMember ioMember) {
    mergeReaders.add(ioMember);
  }

  private void addFetchReaders(IoMember ioMember) {
    fetchReaders.add(ioMember);
  }

  private void addCheckReaders(IoMember ioMember) {
    checkReaders.add(ioMember);
  }

  /**
   * stimulate one read request with good response.
   */
  @Test
  public void goodReadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        logger.warn("reply request: {}", reply.getResponse());
        replies.add(reply);
        NbdRequestResponseGenerator.checkBuffer(reply.getResponse().getBody(), 1);
        if (reply.getResponse().getBody() != null) {
          reply.getResponse().getBody().release();
        }
        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };
    replies.clear();
    ioContextGenerator.setSender(sender);

    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);
    assertEquals(contexts.size(), 3);

    //Check PSSAA
    IoMethodCallback[] callbacks = createCallbacks("PSSAA", 5);

    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    responseGood(callbacks[3]);
    responseGood(callbacks[4]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

    //Check PSSA
    callbacks = createCallbacks("PSSA", 4);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    responseGood(callbacks[3]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

    //Check PSA
    callbacks = createCallbacks("PSA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

    //Check PAA
    callbacks = createCallbacks("PAA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

    when(ioActionContext.isPrimaryDown()).thenReturn(true);
    callbacks = createCallbacks("SSAA", 4);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    responseGood(callbacks[3]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

    when(ioActionContext.isPrimaryDown()).thenReturn(true);
    callbacks = createCallbacks("SAA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);

  }

  /**
   * stimulate one read request with two good responses.
   */
  @Test
  public void twoGoodReadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        replies.add(reply);
        NbdRequestResponseGenerator.checkBuffer(reply.getResponse().getBody(), 1);
        if (reply.getResponse().getBody() != null) {
          reply.getResponse().getBody().release();
        }
        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
      }
    };
    ioContextGenerator.setSender(sender);
    List<IoUnitContext> totalContexts = createReadIoContextManager(volumeId, 125, 4 * pageSize);
    assertEquals(totalContexts.size(), 5);
    List<IoUnitContext> contexts = null;

    // submit the first 3 contexts
    contexts = totalContexts.subList(0, 3);
    ioContextManager = new ReadIoContextManager(volumeId, segId,
        new IoUnitContextPacketImpl(volumeId, contexts, 0, IoRequestType.Read), coordinator);
    ioContextManager.setIoActionContext(ioActionContext);

    IoMethodCallback[] callbacks = createCallbacks("PSSAA", 5);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    responseGood(callbacks[3]);
    responseGood(callbacks[4]);

    assertEquals(replies.size(), 0);
    assertEquals(contexts.get(0).getIoRequest().getReferenceCount(), 2);

    // submit the next 2 contexts
    contexts = totalContexts.subList(3, totalContexts.size());
    ioContextManager = new ReadIoContextManager(volumeId, segId,
        new IoUnitContextPacketImpl(volumeId, contexts, 0, IoRequestType.Read), coordinator);
    ioContextManager.setIoActionContext(ioActionContext);

    callbacks = createCallbacks("PSSAA", 5);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
    responseGood(callbacks[3]);
    responseGood(callbacks[4]);

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(0)).put(any(ResendRequest.class));
  }

  /**
   * meet timeout and fail to response the request.
   */
  @Test
  public void timeoutAndFailResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        logger.info("catch reply: {}", reply);
        replies.add(reply);
        if (reply.getResponse().getBody() != null) {
          reply.getResponse().getBody().release();
        }

        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };
    ioContextGenerator.setSender(sender);
    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);

    assertTrue(contexts.size() == 3);
    // retry
    IoMethodCallback[] callbacks = createCallbacks("PSSAA", 5);

    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    callbacks[2].fail(new TimeoutException());
    callbacks[3].fail(new TimeoutException());
    callbacks[4].fail(new TimeoutException());

    assertEquals(replies.size(), 0);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));

    // timeout and response
    ioContextManager.setExpiredTime(System.currentTimeMillis());
    Thread.sleep(1);

    callbacks = createCallbacks("PSSAA", 5);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    callbacks[2].fail(new TimeoutException());
    callbacks[3].fail(new TimeoutException());
    callbacks[4].fail(new TimeoutException());

    assertEquals(replies.size(), 1);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));

    //Just for unit
    callbacks = createCallbacks("PSA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    responseGood(callbacks[2]);
  }

  /**
   * a request is splited as many request units, some units will be responsed as ok, others will be
   * responsed as failure. the request will be responsed as failure.
   */
  @Test
  public void someGoodAndSomeBadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        replies.add(reply);
        if (reply.getResponse().getBody() != null) {
          logger.warn("releaseBody: {}", reply);
          reply.getResponse().getBody().release();
        }

        assertEquals(reply.getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };

    ioContextGenerator.setSender(sender);
    List<IoUnitContext> totalContexts = createReadIoContextManager(volumeId, 125, 4 * pageSize);
    assertEquals(totalContexts.size(), 5);
    // response three good, two not good.
    int successCount = 3;
    //SSAA so P down
    when(ioActionContext.isPrimaryDown()).thenReturn(true);
    responseNotAllGood_new(successCount, PbIoUnitResult.SKIP);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));
    assertEquals(replies.size(), 0);
    assertEquals(totalContexts.get(0).getIoRequest().getReferenceCount(), 5 - 3);
    for (int i = 0; i < totalContexts.size(); i++) {
      if (i < successCount) {
        assertTrue(totalContexts.get(i).getIoUnit().isSuccess());
      } else {
        assertFalse(totalContexts.get(i).getIoUnit().isSuccess());
      }
    }

    verify(delayResendManager, times(1)).put(any(ResendRequest.class));

  }

  /**
   * the result of the request unit will be outofrange or input no data.
   */
  @Test
  public void badResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        replies.add(reply);
        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };
    ioContextGenerator.setSender(sender);
    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);
    assertTrue(contexts.size() == 3);

    // first time to response fail.
    IoMethodCallback[] callbacks = createCallbacks("PSA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    callbacks[2].fail(new ServerProcessException("one exception"));

    verify(delayResendManager, times(1)).put(any(ResendRequest.class));

    callbacks = createCallbacks("PSA", 3);
    responseGood(callbacks[0]);
    responseGood(callbacks[1]);
    callbacks[2].fail(new TimeoutException());

    verify(delayResendManager, times(2)).put(any(ResendRequest.class));
    assertEquals(replies.size(), 0);

  }

  private List<IoUnitContext> createReadIoContextManager(long volumeId, long offset, int length)
      throws Exception {
    List<IoUnitContext> contexts = ioContextGenerator
        .generateReadIoContexts(volumeId, offset, length);
    IoUnitContextPacket ioUnitContextPacket = new IoUnitContextPacketImpl(volumeId, contexts, 0,
        IoRequestType.Read);
    ioContextManager = new ReadIoContextManager(volumeId, segId, ioUnitContextPacket, coordinator);
    ioContextManager.setExpiredTime(System.currentTimeMillis() + 10000000);
    ioContextManager.setIoActionContext(ioActionContext);
    return contexts;
  }

  //Just for PSSAA

  /**
   * xx.
   */
  public IoMethodCallback[] createCallbacks(String name, int counter) {
    ioContextManager.setIoActionContext(ioActionContext);
    int pnumber = 0;
    int snumber = 0;
    int jnumber = 0;
    int anumber = 0;
    AtomicInteger count = new AtomicInteger(counter);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[counter];
    IoMember ioMember = null;
    String hostName = "10.0.1.";
    int iforHostName = 1;
    long forInstandId = 1L;
    boolean primaryIsDonw = true;
    int callbackNum = 0;
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

    if (pnumber == 1) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Primary,
          Broadcastlog.ReadCause.FETCH);
      addFetchReaders(ioMember);
      callbacks[callbackNum++] = new ReadMethodCallback(ioContextManager, count, coordinator,
          ioMember);
      primaryIsDonw = false;
    }

    for (int i = 0; i < snumber; i++) {
      if (primaryIsDonw) {
        ioMember = new IoMember(new InstanceId(forInstandId++),
            new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Primary,
            Broadcastlog.ReadCause.FETCH);
        addFetchReaders(ioMember);
        primaryIsDonw = false;
      } else {
        ioMember = new IoMember(new InstanceId(forInstandId++),
            new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Secondary,
            Broadcastlog.ReadCause.CHECK);
        addCheckReaders(ioMember);
      }
      callbacks[callbackNum++] = new ReadMethodCallback(ioContextManager, count, coordinator,
          ioMember);
    }

    for (int i = 0; i < jnumber; i++) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Secondary,
          Broadcastlog.ReadCause.CHECK);
      callbacks[callbackNum++] = new ReadMethodCallback(ioContextManager, count, coordinator,
          ioMember);
    }

    for (int i = 0; i < anumber; i++) {
      ioMember = new IoMember(new InstanceId(forInstandId++),
          new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Arbiter,
          Broadcastlog.ReadCause.CHECK);
      callbacks[callbackNum++] = new ReadMethodCallback(ioContextManager, count, coordinator,
          ioMember);
    }

    ioContextManager.initRequestCount(callbacks.length);
    return callbacks;

  }

  private void responseGood(IoMethodCallback callback) {
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = generateContext(ioContextManager.getRequestUnits(),
        PbIoUnitResult.OK);
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), callback.getIoMember().getEndPoint(), pairs);
    callback.complete(response);
  }

  private void responseGood() {
    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Primary,
        Broadcastlog.ReadCause.FETCH);
    // TODO: send count should change
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, new AtomicInteger(1),
        coordinator,
        ioMember);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[1];
    callbacks[0] = callback;
    ioContextManager.initRequestCount(callbacks.length);
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = generateContext(ioContextManager.getRequestUnits(),
        PbIoUnitResult.OK);
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }

  private void responseFail(Exception e) {
    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Primary,
        Broadcastlog.ReadCause.FETCH);
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, new AtomicInteger(1),
        coordinator,
        ioMember);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[1];
    callbacks[0] = callback;
    ioContextManager.initRequestCount(callbacks.length);
    callback.fail(e);
  }

  @SuppressWarnings("unchecked")
  private void responseNotAllGood_new(int successCount, PbIoUnitResult result) {

    //SSAA
    ReadMethodCallback[] callbacks = new ReadMethodCallback[4];
    String hostName = "10.0.2.";
    int forInstandId = 0;
    int iforHostName = 1;
    AtomicInteger atomicInteger = new AtomicInteger(4);
    final List<IoUnitContext> ioContexts = ioContextManager.getRequestUnits();

    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Secondary,
        Broadcastlog.ReadCause.FETCH);
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, atomicInteger,
        coordinator, ioMember);

    callbacks[0] = callback;

    ioMember = new IoMember(new InstanceId(forInstandId++),
        new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Secondary,
        Broadcastlog.ReadCause.CHECK);
    callbacks[1] = new ReadMethodCallback(ioContextManager, atomicInteger, coordinator, ioMember);

    ioMember = new IoMember(new InstanceId(forInstandId++),
        new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Arbiter,
        Broadcastlog.ReadCause.CHECK);
    callbacks[2] = new ReadMethodCallback(ioContextManager, atomicInteger, coordinator, ioMember);

    ioMember = new IoMember(new InstanceId(forInstandId++),
        new EndPoint(hostName + String.valueOf(iforHostName++), 1), MemberIoStatus.Arbiter,
        Broadcastlog.ReadCause.CHECK);
    callbacks[3] = new ReadMethodCallback(ioContextManager, atomicInteger, coordinator, ioMember);

    ioContextManager.initRequestCount(callbacks.length);

    Pair<PbReadResponseUnit, ByteBuf>[] pairs = new Pair[ioContexts.size()];
    Pair<PbReadResponseUnit, ByteBuf>[] tmp = generateContext(ioContexts.subList(0, successCount),
        PbIoUnitResult.OK);
    for (int i = 0; i < successCount; i++) {
      pairs[i] = tmp[i];
    }

    tmp = generateContext(ioContexts.subList(successCount, ioContexts.size()), result);
    for (int i = 0; i < ioContexts.size() - successCount; i++) {
      pairs[successCount + i] = tmp[i];
    }

    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callbacks[0].complete(response);
    callbacks[1].complete(response);
    callbacks[2].complete(response);
    callbacks[3].fail(new TimeoutException());
  }

  @SuppressWarnings("unchecked")
  private void responseNotAllGood(int successCount, PbIoUnitResult result) {
    List<IoUnitContext> ioContexts = ioContextManager.getRequestUnits();
    IoMember ioMember = new IoMember(new InstanceId(RequestIdBuilder.get()), endPoint,
        MemberIoStatus.Primary,
        Broadcastlog.ReadCause.FETCH);
    ReadMethodCallback callback = new ReadMethodCallback(ioContextManager, new AtomicInteger(1),
        coordinator,
        ioMember);
    ReadMethodCallback[] callbacks = new ReadMethodCallback[4];
    callbacks[0] = callback;
    ioContextManager.initRequestCount(callbacks.length);
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = new Pair[ioContexts.size()];
    Pair<PbReadResponseUnit, ByteBuf>[] tmp = generateContext(ioContexts.subList(0, successCount),
        PbIoUnitResult.OK);
    for (int i = 0; i < successCount; i++) {
      pairs[i] = tmp[i];
    }

    tmp = generateContext(ioContexts.subList(successCount, ioContexts.size()), result);
    for (int i = 0; i < ioContexts.size() - successCount; i++) {
      pairs[successCount + i] = tmp[i];
    }

    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }


  /**
   * xx.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Pair<PbReadResponseUnit, ByteBuf>[] generateContext(List<IoUnitContext> contexts,
      PbIoUnitResult result) {
    Pair<PbReadResponseUnit, ByteBuf>[] pairs = new Pair[contexts.size()];
    for (int i = 0; i < contexts.size(); i++) {
      IoUnit ioUnit = contexts.get(i).getIoUnit();
      PbReadRequestUnit unit = PbRequestResponsePbHelper.buildPbReadRequestUnitFrom(ioUnit);
      if (result == PbIoUnitResult.OK) {

        byte[] data = NbdRequestResponseGenerator.getBuffer(ioUnit.getLength(), 1);
        ByteBuf buf = allocator.buffer(ioUnit.getLength());
        buf.writeBytes(data);
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
}
