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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.commons.lang.Validate;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;
import py.common.Utils;
import py.test.TestBase;

/**
 * xx.
 */
public class ByteBufTest extends TestBase {

  public ByteBufTest() throws Exception {
    super.init();
  }

  @Test
  public void composite() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    buf1.writeBytes(new byte[1024]);
    ByteBuf buf2 = Unpooled.directBuffer(300, 300);
    buf2.writeBytes(new byte[300]);
    ByteBuf buf3 = Unpooled.directBuffer(200, 200);
    buf3.writeBytes(new byte[200]);
    ByteBuf buf4 = Unpooled.wrappedBuffer(buf1, buf2, buf3);
    ByteBuffer buffer = buf4.nioBuffer(0, buf4.capacity());
    logger.warn("buffer: {}", buffer);
    buf4.retain();
    logger.warn("fff: {},{}", buf4.refCnt(), buf1.refCnt());
    buf4.release();
    logger.warn("fff: {},{}", buf4.refCnt(), buf1.refCnt());
    buf4.release();
    logger.warn("fff: {},{}", buf4.refCnt(), buf1.refCnt());
  }

  @Test
  public void slice() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    buf1.writeBytes(new byte[1024]);
    ByteBuf buf2 = buf1.slice(buf1.readerIndex(), 512);
    buf2.retain();
    logger.warn("buf1: {}, buf2: {}", buf1.refCnt(), buf2.refCnt());
    buf2.release();
    logger.warn("buf1: {}, buf2: {}", buf1.refCnt(), buf2.refCnt());
    buf1.release();
    logger.warn("buf1: {}, buf2: {}", buf1.refCnt(), buf2.refCnt());
  }

  @Test
  public void order() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    logger.warn("default order: {}", buf1.order());

    byte[] data = new byte[20];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    buf1.writeLong(123456789L);
    buf1.writeInt(123456);
    buf1.writeBytes(data);
    ByteBuf buf3 = buf1.duplicate();
    logger.warn("{}, {}", buf3.readLong(), buf3.readInt());
    for (int i = 0; i < data.length; i++) {
      logger.warn("{} byte: {}", i, buf3.readByte());
      assertEquals(i, data[i]);
    }

    ByteBuf buf4 = buf1.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    logger.warn("{}, {}", buf4.readLong(), buf4.readInt());
    for (int i = 0; i < data.length; i++) {
      logger.warn("{} byte: {}", i, buf4.readByte());
      assertEquals(i, data[i]);
    }
  }

  @Test
  public void release() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    buf1.writeBytes(new byte[1024]);

    ByteBuf buf2 = Unpooled.directBuffer(512, 512);
    buf2.writeBytes(new byte[512]);

    ByteBuf buf3 = Unpooled.wrappedBuffer(buf1, buf2);
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());

    buf3.retain();
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());

    buf3.release();
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());

    buf2.retain();
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());

    buf2.release();
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());

    buf2.release();
    logger.warn("buf1: {}, buf2: {}, buf3: {}", buf1.refCnt(), buf2.refCnt(), buf3.refCnt());
    try {
      buf3.readBytes(new byte[1536]);
      fail();
    } catch (Exception e) {
      logger.warn("it is ok");
    }
  }

  @Test
  public void skipBytes() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    buf1.writeBytes(new byte[1024]);

    ByteBuf buf2 = Unpooled.directBuffer(512, 512);
    buf2.writeBytes(new byte[512]);

    ByteBuf buf3 = Unpooled.wrappedBuffer(buf1, buf2);
    buf3.duplicate().readBytes(new byte[1536]);

    buf2.skipBytes(512);
    buf3.readBytes(new byte[1536]);
  }

  @Test
  public void discardBytes() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    buf1.writeBytes(new byte[1024]);

    ByteBuf buf2 = buf1.slice(512, 512);

    ByteBuf buf3 = Unpooled.directBuffer(512, 512);
    buf3.writeBytes(new byte[512]);

    ByteBuf buf4 = Unpooled.wrappedBuffer(buf2, buf3);
    buf4.readerIndex(buf4.readerIndex() + 510);

    assertEquals(buf1.refCnt(), 1);
    buf4.discardReadBytes();
    assertEquals(buf1.refCnt(), 1);
    buf4.readerIndex(buf4.readerIndex() + 2);
    buf4.discardReadBytes();
    assertEquals(buf1.refCnt(), 0);
    buf4.release();

    assertEquals(buf1.refCnt(), 0);
    assertEquals(buf2.refCnt(), 0);
  }

  @Test
  public void compositeReference() {
    ByteBuf buf1 = Unpooled.directBuffer(1024, 1024);
    assertEquals(buf1.refCnt(), 1);
    buf1.writeBytes(new byte[1024]);

    // one package.
    final ByteBuf package1 = buf1.slice(buf1.readerIndex(), 512);
    buf1.retain();
    buf1.skipBytes(512);

    // two package.
    final ByteBuf package2 = buf1.slice(buf1.readerIndex(), 256);
    buf1.retain();
    buf1.skipBytes(256);

    buf1.release();
    buf1.retain();

    ByteBuf buf2 = Unpooled.directBuffer(512);
    buf2.writeBytes(new byte[512]);
    final ByteBuf buf3 = Unpooled.wrappedBuffer(buf1, buf2);
    buf2.retain();

    buf2.release();

    ByteBuf buf4 = Unpooled.directBuffer(512);
    buf4.writeBytes(new byte[512]);

    ByteBuf buf5 = buf4.slice(buf4.readerIndex(), 256);
    final ByteBuf package3 = Unpooled.wrappedBuffer(buf3, buf5);
    buf4.retain();

    buf4.release();

    final ByteBuf package4 = buf5;
    buf5.retain();

    assertEquals(package1.readableBytes(), 512);
    package1.release();
    assertEquals(package2.readableBytes(), 256);
    package2.release();
    assertEquals(package3.readableBytes(), 1024);
    package3.release();
    assertEquals(package4.readableBytes(), 256);
    package4.release();

    assertEquals(buf1.refCnt(), 0);
    assertEquals(buf2.refCnt(), 0);
    assertEquals(buf3.refCnt(), 0);
    assertEquals(buf4.refCnt(), 0);
    assertEquals(buf5.refCnt(), 0);
  }

  @Test
  public void testReferenceCount() {
    ByteBuf buf1 = Unpooled.wrappedBuffer(new byte[10]);
    final ByteBuf buf2 = buf1.duplicate();
    Validate.isTrue(buf1.refCnt() == 1);
    buf1.release();
    Validate.isTrue(buf1.refCnt() == 0);
    Validate.isTrue(buf2.refCnt() == 0);

    ByteBuf buf3 = Unpooled.wrappedBuffer(new byte[10]);
    ByteBuf buf4 = buf3.duplicate();
    buf4.release();
    Validate.isTrue(buf3.refCnt() == 0);
    Validate.isTrue(buf4.refCnt() == 0);

    ByteBuf buf5 = Unpooled.wrappedBuffer(new byte[10]);
    ByteBuf buf6 = buf5.slice(0, 5);
    buf6.release();
    Validate.isTrue(buf5.refCnt() == 0);
    Validate.isTrue(buf6.refCnt() == 0);

    ByteBuf buf7 = Unpooled.wrappedBuffer(new byte[10]);
    ByteBuf buf8 = buf7.slice(0, 5);
    buf7.release();
    Validate.isTrue(buf7.refCnt() == 0);
    Validate.isTrue(buf8.refCnt() == 0);

    ByteBuf buf9 = Unpooled.wrappedBuffer(new byte[10]);
    buf9.release();
    try {
      buf9.release();
      assertTrue(false);
    } catch (Exception e) {
      logger.error("caught exception", e);
    }

    ByteBuf buf10 = Unpooled.wrappedBuffer(new byte[10]);
    ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(buf10.array(), 1, 5);
    buf10.release();
    channelBuffer.clear();
    channelBuffer.writeByte(1);

  }

  @Test
  public void testChannelBuffer() {
    ChannelBuffer buffer1 = ChannelBuffers.wrappedBuffer(new byte[1]);

    ChannelBuffer buffer2 = ChannelBuffers.wrappedBuffer(new byte[1]);

    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buffer1, buffer2);
    try {
      buffer.array();
      assertTrue(false);
    } catch (UnsupportedOperationException e) {
      logger.warn("caught an exception", e);
    }

    ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[1]);
    logger.info("byteBuf: {}", byteBuf);
  }

  @Test
  public void testByteBufAndCrc32() {
    ByteBuf byteBuf1 = generateByteBuf(1024);
    ByteBuf byteBuf2 = generateByteBuf(1024);
    CRC32 crc32 = new CRC32();
    crc32.update(byteBuf1.array(), byteBuf1.arrayOffset() + byteBuf1.readerIndex(),
        byteBuf1.readableBytes());
    long value1 = crc32.getValue();
    logger.info("crc32 1 value: {}", value1);
    crc32.update(byteBuf2.array(), byteBuf2.arrayOffset() + byteBuf2.readerIndex(),
        byteBuf2.readableBytes());
    value1 = crc32.getValue();
    logger.info("crc32 2 value: {}", value1);

    ByteBuf byteBuf3 = Unpooled.buffer(2048);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuf1, byteBuf2);
    byteBuf3.writeBytes(byteBuf.duplicate());
    crc32 = new CRC32();
    crc32.update(byteBuf3.array(), byteBuf3.arrayOffset() + byteBuf3.readerIndex(),
        byteBuf3.readableBytes());
    long value2 = crc32.getValue();
    logger.info("crc32 3 value: {}", value2);

    assertTrue(value1 == value2);

  }

  @Test
  public void testArrayByteBufAndCrc32() {
    ByteBuf byteBuf1 = generateArrayByteBuf(1024);
    ByteBuf byteBuf2 = generateArrayByteBuf(1024);
    CRC32 crc32 = new CRC32();
    crc32.update(byteBuf1.array(), byteBuf1.arrayOffset() + byteBuf1.readerIndex(),
        byteBuf1.readableBytes());
    long value1 = crc32.getValue();
    logger.info("crc32 1 value: {}", value1);
    crc32.update(byteBuf2.array(), byteBuf2.arrayOffset() + byteBuf2.readerIndex(),
        byteBuf2.readableBytes());
    value1 = crc32.getValue();
    logger.info("crc32 2 value: {}", value1);

    ByteBuf byteBuf3 = Unpooled.buffer(2048);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuf1, byteBuf2);
    byteBuf3.writeBytes(byteBuf.duplicate());
    crc32 = new CRC32();
    crc32.update(byteBuf3.array(), byteBuf3.arrayOffset() + byteBuf3.readerIndex(),
        byteBuf3.readableBytes());
    long value2 = crc32.getValue();
    logger.info("crc32 3 value: {}", value2);

    assertTrue(value1 == value2);
  }

  @Test
  public void testRandom() {
    int maxValue = 4000;
    int minValue = 2500;
    long randValue = 0;
    for (int i = 0; i < 10000; i++) {
      randValue = Utils.generateRandomNum(minValue, maxValue);
      assertTrue(randValue <= maxValue);
      assertTrue(randValue >= minValue);
    }
  }

  @Test
  public void testChannelBufferRead() {
    byte[] genArray = new byte[20];
    for (int i = 0; i < genArray.length; i++) {
      genArray[i] = (byte) i;
    }
    ChannelBuffer buffer1 = ChannelBuffers.wrappedBuffer(genArray);
    buffer1.markReaderIndex();
    byte[] dstArray1 = new byte[5];
    buffer1.readBytes(dstArray1, 0, 5);
    buffer1.resetReaderIndex();
    logger.info("dstArray1: {}", dstArray1);

    byte[] dstArray2 = new byte[5];
    buffer1.readBytes(dstArray2);
    logger.info("dstArray2: {}", dstArray2);

    assertTrue(Arrays.equals(dstArray1, dstArray2));
  }


  /**
   * xx.
   */
  public ByteBuf generateArrayByteBuf(int size) {
    byte[] buf = new byte[size * 2];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(buf, size / 2, size);
    byteBuf.writerIndex(0);
    for (int i = 0; i < size; i++) {
      byteBuf.writeByte(i);
    }

    return byteBuf;
  }


  /**
   * xx.
   */
  public ByteBuf generateByteBuf(int size) {
    ByteBuf byteBuf = Unpooled.buffer(size);
    for (int i = 0; i < size; i++) {
      byteBuf.writeByte(i);
    }

    return byteBuf;
  }
}
