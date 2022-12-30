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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.common.DelayManager;
import py.common.RequestIdBuilder;
import py.common.Utils;
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
import py.coordinator.logmanager.ReadIoContextManager;
import py.coordinator.logmanager.ReadMethodCallback;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.Reply;
import py.coordinator.task.ResendRequest;
import py.coordinator.utils.DummyNetworkDelayRecorder;
import py.coordinator.utils.NetworkDelayRecorder;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;
import py.membership.MemberIoStatus;
import py.membership.SegmentForm;
import py.netty.datanode.PyReadResponse;
import py.netty.exception.ServerProcessException;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.test.TestBase;
import py.volume.VolumeType;

/**
 * xx.
 */
public class ReadIoContextManagerLinkedCloneTest extends TestBase {

  private final Random random = new Random();
  private ReadIoContextManager ioContextManager;
  private SimplePooledByteBufAllocator allocator = new SimplePooledByteBufAllocator(
      16 * 1024 * 1024, 4 * 1024);
  private DelayManager delayResendManager = mock(DelayManager.class);
  private Coordinator coordinator = mock(Coordinator.class);
  private EndPoint endPoint = mock(EndPoint.class);
  private SegId segId = mock(SegId.class);
  private IoContextGenerator ioContextGenerator;
  private long segmentSize = 1024 * 1024;
  private int pageSize = 128;
  private int pageCount;
  private int littlePageCount;
  private List<Reply> replies = new ArrayList<>();
  private IoActionContext ioActionContext = mock(IoActionContext.class);
  private Long volumeId = RequestIdBuilder.get();
  private SpaceSavingVolumeMetadata volumeMetadata = mock(SpaceSavingVolumeMetadata.class);

  private NetworkDelayRecorder networkDelayRecorder = new DummyNetworkDelayRecorder();


  /**
   * xx.
   */
  public ReadIoContextManagerLinkedCloneTest() throws Exception {
    super.init();
    ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize);
    when(coordinator.getDelayManager()).thenReturn(delayResendManager);
    when(coordinator.getVolumeType(any(Long.class))).thenReturn(VolumeType.REGULAR);
    pageCount = allocator.getAvailableMediumPageCount();
    littlePageCount = allocator.getAvailableLittlePageCount();
  }


  /**
   * xx.
   */
  @Before
  public void beforeMethod() {
    // TODO: SegmentForm.PJ
    when(ioActionContext.getSegmentForm()).thenReturn(SegmentForm.PJ);
    Set<IoMember> ioMembers = new HashSet<>();
    ioMembers
        .add(new IoMember(new InstanceId(RequestIdBuilder.get()), new EndPoint("127.0.0.1", 6789),
            MemberIoStatus.Primary, Broadcastlog.ReadCause.FETCH));
    when(ioActionContext.getIoMembers()).thenReturn(ioMembers);
    when(ioActionContext.getRealReaders()).thenReturn(ioMembers);
    when(ioActionContext.getFetchReader()).thenReturn(ioMembers);
    when(coordinator.getNetworkDelayRecorder()).thenReturn(networkDelayRecorder);
    when(coordinator.isLinkedCloneVolume(any())).thenReturn(true);
    when(coordinator.getVolumeMetaData(any())).thenReturn(volumeMetadata);
    when(volumeMetadata.getSourceVolumeId()).thenReturn(1L);
    when(volumeMetadata.getSourceSnapshotId()).thenReturn(1);
    replies.clear();
  }

  @After
  public void afterMethod() {
    assertEquals(pageCount, allocator.getAvailableMediumPageCount());
    assertEquals(littlePageCount, allocator.getAvailableLittlePageCount());
  }

  /**
   * stimulate one read request with good response.
   */
  @Test
  public void oneGoodReadResponse() throws Exception {
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
    ioContextGenerator.setSender(sender);

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        readIoContextManager.setIoActionContext(ioActionContext);
        responseGood(readIoContextManager);
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);
    assertEquals(contexts.size(), 3);

    responseFree();

    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));
  }

  /**
   * stimulate one read request with bad response.
   */
  @Test
  public void badReadResponse() throws Exception {
    NbdResponseSender sender = new NbdResponseSender() {
      @Override
      public void send(Reply reply) {
        logger.warn("reply request: {}", reply.getResponse());
        replies.add(reply);
        assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
      }
    };
    ioContextGenerator.setSender(sender);

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        Thread.sleep(30);
        readIoContextManager.setIoActionContext(ioActionContext);
        responseFail(readIoContextManager, new ServerProcessException(""));
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);
    assertEquals(contexts.size(), 3);

    responseFree();

    Utils.waitUntilConditionMatches(30, () -> {
      return replies.size() == 1;
    });
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
  }

  /**
   * stimulate one read request with good response.
   */
  @Test
  public void someUnitGoodResponseAndSomeUnitFreeResponseTest() throws Exception {
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
    ioContextGenerator.setSender(sender);

    List<ResendRequest> resendRequests = new ArrayList<>();
    AtomicInteger count = new AtomicInteger(3);
    AtomicInteger index = new AtomicInteger(0);
    doAnswer(invocationOnMock -> {
      ResendRequest request = invocationOnMock.getArgument(0);
      logger.debug("get a resend request {}", request);
      if (request.getIoContextManager() instanceof ReadIoContextManager) {
        ReadIoContextManager readIoContextManager = (ReadIoContextManager) request
            .getIoContextManager();

        readIoContextManager.setIoActionContext(ioActionContext);
        responseGood(readIoContextManager);
      } else {
        resendRequests.add(request);
      }

      return request;
    }).when(delayResendManager).put(any(Delayed.class));

    List<IoUnitContext> contexts = createReadIoContextManager(volumeId, 127, 2 * pageSize);
    assertEquals(contexts.size(), 3);

    responseSomeFreeAndGood(1);

    assertEquals(replies.size(), 1);
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.SUCCEEDED);
    verify(delayResendManager, times(1)).put(any(ResendRequest.class));
  }

  private List<IoUnitContext> createReadIoContextManager(long volumeId, long offset, int length)
      throws Exception {
    List<IoUnitContext> contexts = ioContextGenerator
        .generateReadIoContexts(volumeId, offset, length);
    IoUnitContextPacket ioUnitContextPacket = new IoUnitContextPacketImpl(volumeId, contexts, 0,
        IoRequestType.Read);
    ioContextManager = new ReadIoContextManager(volumeId, segId, ioUnitContextPacket, coordinator);
    ioContextManager.setExpiredTime(System.currentTimeMillis() + 10000);
    ioContextManager.setIoActionContext(ioActionContext);
    return contexts;
  }

  private void responseFree() {
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
        PbIoUnitResult.FREE);
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }

  private void responseSomeFreeAndGood(int freeNumber) {
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

    Pair<PbReadResponseUnit, ByteBuf>[] pairs = new Pair[ioContextManager.getRequestUnits().size()];
    for (int i = 0; i < ioContextManager.getRequestUnits().size(); i++) {
      if (freeNumber-- > 0) {
        Pair<PbReadResponseUnit, ByteBuf> pair = generateContext(
            ioContextManager.getRequestUnits().get(i),
            PbIoUnitResult.FREE);
        pairs[i] = pair;
      } else {
        Pair<PbReadResponseUnit, ByteBuf> pair = generateContext(
            ioContextManager.getRequestUnits().get(i),
            PbIoUnitResult.OK);
        pairs[i] = pair;
      }
    }
    PyReadResponse response = NbdRequestResponseGenerator
        .generateReadResponse(RequestIdBuilder.get(), endPoint, pairs);
    callback.complete(response);
  }

  private void responseGood(ReadIoContextManager ioContextManager) {
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

  private void responseFail(ReadIoContextManager ioContextManager, Exception e) {
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


  /**
   * xx.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Pair<PbReadResponseUnit, ByteBuf> generateContext(IoUnitContext context,
      PbIoUnitResult result) {
    IoUnit ioUnit = context.getIoUnit();
    PbReadRequestUnit unit = PbRequestResponsePbHelper.buildPbReadRequestUnitFrom(ioUnit);
    if (result == PbIoUnitResult.OK) {

      byte[] data = NbdRequestResponseGenerator.getBuffer(ioUnit.getLength(), 1);
      ByteBuf buf = allocator.buffer(ioUnit.getLength());
      buf.writeBytes(data);
      PbReadResponseUnit responseUnit = PbRequestResponseHelper
          .buildPbReadResponseUnitFrom(unit, buf);
      Pair<PbReadResponseUnit, ByteBuf> pair = new Pair(responseUnit, buf);
      return pair;
    } else {
      PbReadResponseUnit responseUnit = PbRequestResponseHelper
          .buildPbReadResponseUnitFrom(unit, result);
      Pair<PbReadResponseUnit, ByteBuf> pair = new Pair(responseUnit, null);
      return pair;
    }
  }
}
