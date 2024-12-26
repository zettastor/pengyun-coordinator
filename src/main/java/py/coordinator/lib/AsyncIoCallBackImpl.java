/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
