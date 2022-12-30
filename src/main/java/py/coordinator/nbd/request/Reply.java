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