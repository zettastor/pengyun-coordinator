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
