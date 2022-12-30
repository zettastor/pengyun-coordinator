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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.Coordinator;
import py.coordinator.nbd.NbdRequestFrameDispatcher;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.Reply;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.nbd.request.Response;
import py.exception.StorageException;
import py.storage.Storage;


@Deprecated
public class FakeStorage extends Storage {

  private static Logger logger = LoggerFactory.getLogger(FakeStorage.class);
  @SuppressWarnings("unused")
  private final NbdConfiguration nbdConfiguration;
  private final Map<Long, RequestMessage> mapRequestToLatch;
  private final Coordinator coordinator;
  private final NbdResponseSender sender;
  private final NbdRequestFrameDispatcher dispatcher;

  private final AtomicLong requestIdGenerator;



  public FakeStorage(NbdConfiguration nbdConfiguration, Coordinator coordinator) {
    super("CoordinatorBaseTest " + System.currentTimeMillis());
    this.nbdConfiguration = nbdConfiguration;
    this.sender = new NbdResponseSender1();
    this.dispatcher = new NbdRequestFrameDispatcher1(coordinator, sender);
    this.mapRequestToLatch = new ConcurrentHashMap<>();
    this.coordinator = coordinator;
    this.requestIdGenerator = new AtomicLong(0);
  }



  public static Request generateReadRequest(long requestId, long offset, int length) {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Read, requestId, offset,
        length, 1);
    return new Request(requestHeader, null);
  }



  public static Request generateWriteRequest(long requestId, long offset, int length, byte[] data) {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Write, requestId,
        offset, length, 1);
    return new Request(requestHeader, Unpooled.wrappedBuffer(data));
  }



  public void close() {
    try {
      coordinator.close();
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }
    sender.stop();
  }

  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    long requestId = requestIdGenerator.getAndIncrement();
    logger.info("read request Id: {}, offset:{}, len:{}", requestId, pos, len);

    Request request = generateReadRequest(requestId, off, len);
    List<Request> requsets = new ArrayList<Request>();
    requsets.add(request);

    RequestMessage message = new RequestMessage(new CountDownLatch(1));
    mapRequestToLatch.put(requestId, message);
    try {
      dispatcher.channelRead(new NbdChannelHandlerContext<Channel>(null), requsets);
    } catch (Exception e) {
      logger.error("can not read data, offset: {}, len: {}", pos, len, e);
      throw new StorageException("can not send request, pos=" + pos + ",length=" + len);
    }

    message.await();

    logger.info("coordinator read request: {} has response", requestId);

    Response response = message.getReply().getResponse();
    ByteBuf channelBuffer = response.getBody();

    if (response.getErrCode() != ProtocoalConstants.SUCCEEDED) {
      logger.warn("reply error no is {}", response.getErrCode());
      throw new StorageException("read fail, pos=" + pos + ", len=" + len);
    }

    channelBuffer.getBytes(0, dstBuf, off, len);
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    read(pos, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    long requestId = requestIdGenerator.getAndIncrement();
    logger.info("write request Id: {}, offset:{}, len:{}", requestId, pos, len);
    Request request = generateWriteRequest(requestId, off, len, buf);
    List<Request> requsets = new ArrayList<Request>();
    requsets.add(request);

    RequestMessage message = new RequestMessage(new CountDownLatch(1));
    mapRequestToLatch.put(requestId, message);

    try {
      dispatcher.channelRead(new NbdChannelHandlerContext<Channel>(null), requsets);
    } catch (Exception e) {
      logger.error("can not write data, offset: {}, len: {}", pos, len);
      throw new StorageException("write fail, pos=" + pos + ", len=" + len);
    }

    message.await();

    Reply reply = message.getReply();
    if (reply.getResponse().getErrCode() != ProtocoalConstants.SUCCEEDED) {
      throw new StorageException();
    }
    logger.info("coordinator write request: {} has response", requestId);
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    write(pos, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
  }

  @Override
  public long size() {
    return coordinator.size();
  }

  public Coordinator getCoordinator() {
    return this.coordinator;
  }

  private class RequestMessage {

    private final CountDownLatch latch;
    private Reply reply;

    public RequestMessage(CountDownLatch latch) {
      this.latch = latch;
    }

    public void countDown() {
      latch.countDown();
    }

    public void await() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        logger.error("caught an exception", e);
      }
    }

    public Reply getReply() {
      return reply;
    }

    public void setReply(Reply reply) {
      this.reply = reply;
    }
  }

  private class NbdResponseSender1 extends NbdResponseSender {

    public NbdResponseSender1() {
    }

    @Override
    public void send(Reply reply) {
      long requestId = reply.getResponse().getHandler();
      logger.info("response request: {}", requestId);
      RequestMessage message = mapRequestToLatch.remove(requestId);
      Validate.notNull(message);
      message.setReply(reply);
      message.countDown();
    }
  }

  private class NbdRequestFrameDispatcher1 extends NbdRequestFrameDispatcher {

    public NbdRequestFrameDispatcher1(Coordinator coordinator, NbdResponseSender sender) {
      super(coordinator, sender);
    }

    @Override
    public boolean checkPermission(ChannelHandlerContext ctx, Request request) {
      return true;
    }
  }
}
