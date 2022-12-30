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
import py.coordinator.utils.GetMicrosecondTimestamp;


public class DebugResponse extends Response {

  private long nbdServerTimestamp;
  private long nbdClientTimestamp;

  public DebugResponse(ReplyMagic magicType, int errCode, long handler, ByteBuf body) {
    super(magicType, errCode, handler, body);
  }

  public long getNbdServerTimestamp() {
    return nbdServerTimestamp;
  }

  public void setNbdServerTimestamp(long nbdServerTimestamp) {
    this.nbdServerTimestamp = nbdServerTimestamp;
  }

  public long getNbdClientTimestamp() {
    return nbdClientTimestamp;
  }

  public void setNbdClientTimestamp(long nbdClientTimestamp) {
    this.nbdClientTimestamp = nbdClientTimestamp;
  }

  @Override
  public ByteBuf writeTo(ByteBuf buffer) {
   
    buffer.writeInt(getReplyMagic().getValue());
    buffer.writeInt(getErrCode());
    buffer.writeLong(getHandler());

   
   
   
    buffer.writeLong(GetMicrosecondTimestamp.getCurrentTimeMicros());
    ByteBuf body = getBody();
    return (body != null) ? Unpooled.wrappedBuffer(buffer, body) : buffer;
  }
}









