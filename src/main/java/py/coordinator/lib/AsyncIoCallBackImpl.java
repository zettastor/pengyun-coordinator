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

package py.coordinator.lib;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AsyncIoCallBackImpl implements AsyncIoCallBack {

  private static final Logger logger = LoggerFactory.getLogger(AsyncIoCallBackImpl.class);

  private Semaphore semaphore;

  private ByteBuf byteBuf;



  public AsyncIoCallBackImpl(Semaphore semaphore) {
    Validate.notNull(semaphore);
    this.semaphore = semaphore;
    try {
      this.semaphore.acquire();
    } catch (InterruptedException e) {
      logger.error("caught an exception", e);
    }
  }

  @Override
  public void ioRequestDone(Long volumeId, boolean result, ByteBuf byteBuf) {
    try {
      if (!result && this.byteBuf != null) {
        Validate.notNull(byteBuf);
        logger.debug("origin byte buf, read index:{}, write index:{}, capacity:{}",
            byteBuf.readerIndex(),
            byteBuf.writerIndex(), byteBuf.capacity());
        logger.debug("dst byte buf, read index:{}, write index:{}, capacity:{}",
            this.byteBuf.readerIndex(),
            this.byteBuf.writerIndex(), this.byteBuf.capacity());
        this.byteBuf.writeBytes(byteBuf);
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
    } finally {
      this.semaphore.release();
    }
  }

  @Override
  public void setReadDstBuffer(ByteBuf buffer) {
    Validate.notNull(buffer);
    this.byteBuf = buffer;

    this.byteBuf.resetWriterIndex();
  }

  @Override
  public Channel getChannel() {

    return null;
  }

}
