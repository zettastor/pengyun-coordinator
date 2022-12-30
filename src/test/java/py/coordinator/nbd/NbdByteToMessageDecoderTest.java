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

package py.coordinator.nbd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.test.TestBase;

/**
 * xx.
 */
public class NbdByteToMessageDecoderTest extends TestBase {

  private ChannelHandlerContext context = mock(ChannelHandlerContext.class);

  public NbdByteToMessageDecoderTest() throws Exception {
    super.init();
    when(context.fireChannelRead(any())).thenReturn(null);
  }

  @Test
  public void base() throws Exception {
    final long offset = 1000;
    final int length = 10;
    final AtomicInteger counter = new AtomicInteger(0);

    NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder() {
      @Override
      public void fireChannelRead(RequestHeader object, ByteBuf body) {
        logger.info("request: {}", object);
        Validate.isTrue(offset == object.getOffset());
        Validate.isTrue(length == object.getLength());
        Validate.isTrue(object.getRequestType() == NbdRequestType.Read);
        counter.addAndGet(1);
        super.fireChannelRead(object, body);
      }
    };

    int count = 2;
    try {
      Random random = new Random(10);
      for (int i = 0; i < count; i++) {
        byte[] data = NbdRequestResponseGenerator.generateReadRequestPlan(offset, length);
        for (int j = 0; j < data.length; j++) {
          decoder.channelRead(context, Unpooled.wrappedBuffer(data, j, 1));
          if (random.nextInt(10) >= 8) {
            decoder.channelRead(context, Unpooled.wrappedBuffer(data, j, 0));
          }
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    }

    assertEquals(counter.get(), count);
  }

  @Test
  public void parseHeader() throws Exception {
    final long offset = 1000;
    final int length = 10;
    final AtomicInteger counter = new AtomicInteger(0);

    NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder() {
      @Override
      public void fireChannelRead(RequestHeader header, ByteBuf body) {
        logger.warn("request: {}, {}", header, body);
        Validate.isTrue(offset == header.getOffset());
        Validate.isTrue(length == header.getLength());
        Validate.isTrue(header.getRequestType() == NbdRequestType.Read);
        counter.addAndGet(1);
        super.fireChannelRead(header, body);
      }
    };

    int count = 2;
    try {
      for (int i = 0; i < count; i++) {
        byte[] data = NbdRequestResponseGenerator.generateReadRequestPlan(offset, length);
        decoder.channelRead(context, Unpooled.wrappedBuffer(data));
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }

    assertTrue(counter.get() == count);
  }

  @Test
  public void parseData() throws Exception {
    final long offset = 1000;
    final int length = 10;
    final int delta = 5;
    final AtomicInteger counter = new AtomicInteger(0);
    doAnswer(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        Validate.isTrue(args.length == 1);
        @SuppressWarnings("unchecked") Collection<Request> requests = (Collection<Request>) args[0];
        for (Request object : requests) {
          logger.info("request: {}", object);
          Validate.isTrue(offset == object.getHeader().getOffset());
          Validate.isTrue(length == object.getHeader().getLength());
          Validate.isTrue(object.getHeader().getRequestType() == NbdRequestType.Write);
          ByteBuf body = object.getBody();
          Validate.isTrue(body.readableBytes() == object.getHeader().getLength());
          NbdRequestResponseGenerator.checkBuffer(body, delta);
        }
        counter.addAndGet(requests.size());
        return null;
      }
    }).when(context).fireChannelRead(any());

    int count = 2;

    try {
      NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder();
      for (int i = 0; i < count; i++) {
        byte[] data = NbdRequestResponseGenerator
            .generateWriteRequestPlan(offset, length,
                NbdRequestResponseGenerator.getBuffer(length, delta));
        decoder.channelRead(context, Unpooled.wrappedBuffer(data));
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }

    assertTrue(counter.get() == count);
  }

  @Test
  public void randomAddAndParse() throws Exception {
    int count = 1; // 000;
    final int requestOffset = 1000;
    final int requestLength = 1024;
    byte[] request = new byte[1024 * 1500];
    Random random = new Random();
    final int delta = 7;
    int offset = 0;
    for (int i = 0; i < count; i++) {
      byte[] data = null;
      if (random.nextBoolean()) {
        data = NbdRequestResponseGenerator.generateWriteRequestPlan(0, requestOffset, requestLength,
            NbdRequestResponseGenerator.getBuffer(requestLength, delta));
      } else {
        data = NbdRequestResponseGenerator.generateReadRequestPlan(requestOffset, requestLength);
      }
      System.arraycopy(data, 0, request, offset, data.length);
      offset += data.length;
    }

    final AtomicInteger counter = new AtomicInteger(0);
    NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder();
    doAnswer(new Answer<ChannelFuture>() {
      @Override
      public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        Validate.isTrue(args.length == 1);
        @SuppressWarnings("unchecked") Collection<Request> requests = (Collection<Request>) args[0];
        for (Request object : requests) {
          logger.info("request: {}", object);
          Validate.isTrue(requestOffset == object.getHeader().getOffset());
          Validate.isTrue(requestLength == object.getHeader().getLength());
          if (object.getHeader().getRequestType() == NbdRequestType.Write) {
            ByteBuf body = object.getBody();
            Validate.isTrue(body.readableBytes() == object.getHeader().getLength());
            NbdRequestResponseGenerator.checkBuffer(body, delta);
          }
        }
        counter.addAndGet(requests.size());
        return null;
      }
    }).when(context).fireChannelRead(any());

    int tmpOffset = 0;

    while (tmpOffset < offset) {
      int maxLength = offset - tmpOffset + 1;
      maxLength = (maxLength > 5000 ? 5000 : maxLength);
      int tmpLength = random.nextInt(maxLength);
      logger.info("offset={}, length={}", tmpOffset, tmpLength);
      ByteBuf tmp = Unpooled.wrappedBuffer(request, tmpOffset, tmpLength);
      try {
        decoder.channelRead(context, tmp);
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }

      tmpOffset += tmpLength;
    }

    assertTrue("count: " + counter.get() + ", expected: " + count, counter.get() == count);
  }
}
