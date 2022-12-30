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
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.DebugResponse;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.Reply;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.nbd.request.Response;



public class NbdAsyncIoCallBack implements AsyncIoCallBack {

  private static final Logger logger = LoggerFactory.getLogger(NbdAsyncIoCallBack.class);
  private final Channel channel;
  private final RequestHeader requestHeader;
  private final NbdResponseSender sender;
 
  private final long nbdServerTimestamp;


  
  public NbdAsyncIoCallBack(Channel channel, RequestHeader requestHeader,
      NbdResponseSender sender) {
    this.channel = channel;
    this.requestHeader = requestHeader;
    this.sender = sender;
    this.nbdServerTimestamp = System.currentTimeMillis();
  }

  @Override
  public void ioRequestDone(Long volumeId, boolean result, ByteBuf byteBuf) {
    int errCode = result ? ProtocoalConstants.EIO : ProtocoalConstants.SUCCEEDED;
    Reply reply;
    MagicType magicType = requestHeader.getMagicType();
    if (magicType.isDebug()) {
      DebugResponse response = new DebugResponse(magicType.getReplyMagic(), errCode,
          requestHeader.getHandler(),
          byteBuf);
      response.setNbdClientTimestamp(requestHeader.getNbdClientTimestamp());
      response.setNbdServerTimestamp(nbdServerTimestamp);
      reply = new Reply(response, channel, requestHeader.getRequestType());
    } else {
      reply = new Reply(
          new Response(magicType.getReplyMagic(), errCode, requestHeader.getHandler(), byteBuf),
          channel, requestHeader.getRequestType());
    }

   
    if (CoordinatorConfigSingleton.getInstance().getRwLogFlag()) {
      logger.warn("reply this request:{}, failure:{}", requestHeader, result);
    }

   
    sender.send(reply);
  }

  @Override
  public void setReadDstBuffer(ByteBuf buffer) {
    throw new NotImplementedException("this is NBDAsyncIOCallBack");
  }

  @Override
  public Channel getChannel() {
    return channel;
  }
}
