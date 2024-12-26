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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import py.coordinator.nbd.ProtocoalConstants;
import py.netty.memory.PooledByteBufAllocatorWrapper;


public class Reply {


  protected final Channel channel;
  private final NbdRequestType requestType;
  public ByteBufAllocator allocator = PooledByteBufAllocatorWrapper.INSTANCE;
  protected Response response;



  public Reply(Response response, Channel channel, NbdRequestType requestType) {
    this.response = response;
    this.channel = channel;
    this.requestType = requestType;
  }



  public static Reply generateReply(RequestHeader header, int errCode, ByteBuf body,
      Channel channel) {
    return new Reply(
        new Response(header.getMagicType().getReplyMagic(), errCode, header.getHandler(), body),
        channel, header.getRequestType());
  }



  public static Reply generateHeartbeatReply(RequestHeader header, Channel channel) {
    long handler = 0L;
    if (header != null) {
      handler = header.getHandler();
    }
    return new Reply(
        new Response(ReplyMagic.REPLY_HEARTBEAT, ProtocoalConstants.SUCCEEDED, handler, null),
        channel,
        NbdRequestType.Heartbeat);
  }



  public ByteBuf asByteBuf() {

    ReplyMagic magicType = response.getReplyMagic();
    ByteBuf buffer = allocator.buffer(magicType.getReplyLength());
    return response.writeTo(buffer);
  }

  public Response getResponse() {
    return response;
  }

  public Channel getChannel() {
    return channel;
  }

  public NbdRequestType getRequestType() {
    return requestType;
  }

  @Override
  public String toString() {
    return "Reply{" + "response=" + response + ", channel=" + channel + ", requestType="
        + requestType + '}';
  }
}