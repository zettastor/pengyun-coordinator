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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.DirectAlignedBufferAllocator;
import py.common.FastBuffer;
import py.exception.BufferOverflowException;
import py.exception.BufferUnderflowException;
import sun.nio.ch.DirectBuffer;

@Deprecated
public class FileFastBufferForCoordinator implements FastBuffer {

  private static final Logger logger = LoggerFactory.getLogger(FileFastBufferForCoordinator.class);

  private long accessibleMemSize = 0L;
  private RandomAccessFile raf;



  public FileFastBufferForCoordinator(String filePath, long size) {
    this.accessibleMemSize = size;
    try {
      this.raf = new RandomAccessFile(new File(filePath), "rw");
    } catch (FileNotFoundException e) {
      logger.error("open file failed", e);
      System.exit(-1);
    }
  }



  public FileFastBufferForCoordinator(String filePath) {
    try {
      this.raf = new RandomAccessFile(new File(filePath), "rw");
      this.accessibleMemSize = this.raf.length();
    } catch (IOException e) {
      logger.error("open file failed", e);
      System.exit(-1);
    }
  }

  @Override
  public void get(byte[] dst) throws BufferUnderflowException {
    get(dst, 0, dst.length);
  }

  @Override
  public void get(byte[] dst, int offset, int length) throws BufferUnderflowException {
    get(0, dst, offset, length);
  }

  @Override
  public void get(long srcOffset, byte[] dst, int dstOffset, int length)
      throws BufferUnderflowException {
    if (srcOffset + length > accessibleMemSize) {
      throw new BufferUnderflowException(
          "dst' length " + length + " is larger than the buffer size "
              + accessibleMemSize + ", offset: " + srcOffset);
    }

    synchronized (raf) {
      try {
        raf.seek(srcOffset);
        raf.read(dst, dstOffset, length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void get(ByteBuffer dst) throws BufferUnderflowException {
    get(0, dst, dst.position(), dst.remaining());
  }

  @Override
  public void get(ByteBuffer dst, int dstOffset, int length) throws BufferUnderflowException {
    get(0, dst, dstOffset, length);
  }

  @Override
  public void get(long srcOffset, ByteBuffer dstBuffer, int dstOffset, int length)
      throws BufferUnderflowException {
    if (srcOffset + length > accessibleMemSize) {
      throw new BufferUnderflowException(
          "dst' length " + length + " is larger than the buffer size "
              + accessibleMemSize + ", offset: " + srcOffset);
    }

    try {
      if (dstBuffer.isDirect()) {
        ByteBuffer srcBuffer = raf.getChannel().map(MapMode.READ_ONLY, srcOffset, length);
        DirectAlignedBufferAllocator.copyMemory(((DirectBuffer) srcBuffer).address(),
            ((DirectBuffer) dstBuffer).address() + dstOffset, length);
      } else {
        Validate.isTrue(dstBuffer.hasArray());
        get(srcOffset, dstBuffer.array(), dstBuffer.arrayOffset() + dstOffset, length);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(byte[] src) throws BufferOverflowException {
    put(src, 0, src.length);
  }

  @Override
  public void put(byte[] src, int offset, int length) throws BufferOverflowException {
    put(0, src, offset, length);
  }

  @Override
  public void put(long dstOffset, byte[] src, int srcOffset, int length)
      throws BufferOverflowException {
    if (dstOffset + length > accessibleMemSize) {
      throw new BufferOverflowException("src' length " + length + " is larger than the buffer size "
          + this.accessibleMemSize + ", dstOffset: " + dstOffset);
    }

    synchronized (raf) {
      try {
        raf.seek(dstOffset);
        raf.write(src, srcOffset, length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void put(ByteBuffer src) throws BufferOverflowException {
    put(src, src.position(), src.remaining());
  }

  @Override
  public void put(ByteBuffer src, int srcOffset, int length) throws BufferOverflowException {
    put(0, src, srcOffset, length);
  }

  @Override
  public void put(long dstOffset, ByteBuffer srcBuffer, int srcOffset, int length)
      throws BufferOverflowException {
    if (dstOffset + length > this.accessibleMemSize) {
      throw new BufferOverflowException("src' length " + length + " is larger than the buffer size "
          + this.accessibleMemSize + ", dstOffset: " + dstOffset);
    }

    try {
      if (srcBuffer.isDirect()) {
        ByteBuffer dstBuffer = raf.getChannel().map(MapMode.READ_WRITE, dstOffset, length);
        DirectAlignedBufferAllocator.copyMemory(((DirectBuffer) srcBuffer).address() + srcOffset,
            ((DirectBuffer) dstBuffer).address(), length);
      } else {
        Validate.isTrue(srcBuffer.hasArray());
        put(dstOffset, srcBuffer.array(), srcBuffer.arrayOffset() + srcOffset, length);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long size() {
    return this.accessibleMemSize;
  }

  @Override
  public byte[] array() {
    if (size() == 0) {
      return null;
    }

    try {
      byte[] temp = new byte[(int) size()];
      get(temp);
      return temp;
    } catch (BufferUnderflowException e) {
      logger.error("can't clone an array", e);
      return null;
    }
  }



  public void close() {
    if (this.raf != null) {
      try {
        this.raf.close();
      } catch (IOException e) {
        logger.error("can not close raf", e);
      }
    }
  }
}
