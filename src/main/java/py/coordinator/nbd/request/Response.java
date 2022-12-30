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
