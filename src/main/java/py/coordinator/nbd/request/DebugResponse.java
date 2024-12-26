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









