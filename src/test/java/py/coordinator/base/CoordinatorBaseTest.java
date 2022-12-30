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

package py.coordinator.base;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.local.LocalAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.lib.AsyncIoCallBack;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.NbdRequestFrameDispatcher;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.request.Reply;
import py.coordinator.nbd.request.Request;
import py.exception.StorageException;

/**
 * xx.
 */
public class CoordinatorBaseTest extends StorageDriver {

  private static Logger logger = LoggerFactory.getLogger(CoordinatorBaseTest.class);
  @SuppressWarnings("unused")
  private final NbdConfiguration nbdConfiguration;
  // private final Map<Long, RequestMessage> mapRequestToLatch;
  private final Coordinator coordinator;
  // private final NBDResponseSender sender;
  private final AtomicLong requestIdGenerator;
  // private final NBDRequestFrameDispatcher dispatcher;
  private final Channel channel;
  // private final PYDClientManager pydClientManager;
  private long volumeId;


  /**
   * xx.
   */
  public CoordinatorBaseTest(NbdConfiguration nbdConfiguration, Coordinator coordinator) {
    super("CoordinatorBaseTest");
    this.nbdConfiguration = nbdConfiguration;
    this.channel = mock(Channel.class);
    when(channel.remoteAddress()).thenReturn(new LocalAddress(getRandomString(5)));
    // pydClientManager = new PYDClientManager(2000, false, 30);
    // pydClientManager.clientActive(this.channel, AccessPermissionType.READWRITE);
    // this.sender = new NBDResponseSender1();
    // this.sender.setPydClientManager(pydClientManager);

    // this.dispatcher = new NBDRequestFrameDispatcher1(coordinator, sender);
    // this.dispatcher.setPydClientManager(pydClientManager);
    // this.mapRequestToLatch = new ConcurrentHashMap<>();
    this.coordinator = coordinator;
    this.requestIdGenerator = new AtomicLong(0);
  }

  public Long getVolumeId() {
    throw new NotImplementedException("can not use volume id as key to map coordinator");
  }

  private String getRandomString(int length) {
    String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(base.length());
      sb.append(base.charAt(number));
    }
    return sb.toString();
  }


  /**
   * xx.
   */
  public void close() {
    // sender.stop();
    try {
      // this.pydClientManager.stop();
      coordinator.close();
    } catch (StorageException e) {
      logger.error("can not close the coordinator", e);
    }
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

  @Override
  public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
    long requestId = requestIdGenerator.getAndIncrement();
    logger.warn("read request Id: {}, offset:{}, len:{}", requestId, pos, len);
    coordinator.read(pos, dstBuf, off, len);
    logger.warn("coordinator read request: {} has response", requestId);
  }

  @Override
  public void read(long pos, ByteBuffer buffer) throws StorageException {
    read(pos, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
  }

  @Override
  public void asyncRead(long pos, byte[] dstBuf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    coordinator.asyncRead(pos, dstBuf, off, len, asyncIoCallBack);
  }

  @Override
  public void asyncRead(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    coordinator.asyncRead(pos, buffer, asyncIoCallBack);
  }

  @Override
  public void write(long pos, byte[] buf, int off, int len) throws StorageException {
    long requestId = requestIdGenerator.getAndIncrement();
    logger.info("write request:{} offset:{}, len:{}", requestId, pos, len);

    coordinator.write(pos, buf, off, len);
    logger.info("coordinator write request: {} has response", requestId);
  }

  @Override
  public void write(long pos, ByteBuffer buffer) throws StorageException {
    write(pos, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
  }

  @Override
  public void asyncWrite(long pos, byte[] buf, int off, int len, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    coordinator.asyncWrite(pos, buf, off, len, asyncIoCallBack);
  }

  @Override
  public void asyncWrite(long pos, ByteBuffer buffer, AsyncIoCallBack asyncIoCallBack)
      throws StorageException {
    coordinator.asyncWrite(pos, buffer, asyncIoCallBack);
  }

  @Override
  public long size() {
    return coordinator.size();
  }

  public Coordinator getCoordinator() {
    return this.coordinator;
  }

  @Deprecated
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

  @Deprecated
  private class NbdResponseSender1 extends NbdResponseSender {

    @Override
    public void send(Reply reply) {
      long requestId = reply.getResponse().getHandler();
      logger.info("response request: {}", requestId);
      // = mapRequestToLatch.remove(requestId);
      RequestMessage message = null;
      Validate.notNull(message);
      message.setReply(reply);
      message.countDown();
    }
  }

  @Deprecated
  private class NbdRequestFrameDispatcher1 extends NbdRequestFrameDispatcher {

    public NbdRequestFrameDispatcher1(StorageDriver storageDriver, NbdResponseSender sender) {
      super(storageDriver, sender);
    }

    @Override
    public boolean checkPermission(ChannelHandlerContext ctx, Request request) {
      return true;
    }
  }
}
