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

package py.coordinator.nbd.request;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.utils.GetMicrosecondTimestamp;


public class Response {

  private static final Logger logger = LoggerFactory.getLogger(Response.class);
  private final ReplyMagic replyMagic;
  private final long handler;
  private final int errCode;
  private ByteBuf body;


  
  public Response(ReplyMagic replyMagic, int errCode, long handler, ByteBuf body) {
    this.replyMagic = replyMagic;
    this.handler = handler;
    this.body = body;
    this.errCode = errCode;
  }


  
  public Response(ReplyMagic replyMagic, ByteBuf buffer) {
    this.replyMagic = replyMagic;
    this.errCode = buffer.readInt();
    this.handler = buffer.readLong();
  }

  public ReplyMagic getReplyMagic() {
    return replyMagic;
  }

  public ByteBuf getBody() {
    return body;
  }

  public void setBody(ByteBuf body) {
    this.body = body;
  }

  public long getHandler() {
    return handler;
  }

  public int getErrCode() {
    return errCode;
  }


  
  public ByteBuf writeTo(ByteBuf buffer) {
    buffer.writeInt(replyMagic.getValue());
    buffer.writeInt(errCode);
    buffer.writeLong(handler);
    if (replyMagic == ReplyMagic.REPLY_DEBUG) {
      long nanoTime = GetMicrosecondTimestamp.getCurrentTimeMicros();
      buffer.writeLong(nanoTime);
      logger.info("reply timestamp:{}", nanoTime);
    }
    return (body != null) ? Unpooled.wrappedBuffer(buffer, body) : buffer;
  }

  public int getLength() {
    throw new NotImplementedException("");
  }

  public void releaseBody() {
    this.body = null;
  }

  @Override
  public String toString() {
    return "Response{" + "replyMagic=" + replyMagic + ", handler=" + handler + ", body=" + body
        + ", errCode="
        + errCode + '}';
  }
}
