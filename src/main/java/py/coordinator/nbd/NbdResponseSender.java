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

package py.coordinator.nbd;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.Reply;


public class NbdResponseSender {

  private static final Logger logger = LoggerFactory.getLogger(NbdResponseSender.class);

  private PydClientManager pydClientManager;

  public NbdResponseSender() {
  }



  public void send(Reply reply) {
    Validate.isTrue(reply.getChannel() != null);
    ChannelFuture channelFuture = reply.getChannel().writeAndFlush(reply.asByteBuf());

    reply.getResponse().releaseBody();

    channelFuture.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          logger.warn("fail response:{} to channel:{}", reply.getResponse().getHandler(),
              future.channel());
        }
        logger.info("reply:{} to channel:{}", Long.toHexString(reply.getResponse().getHandler()),
            reply.getChannel().id());
        if (reply.getRequestType() == NbdRequestType.Read) {
          pydClientManager.markReadResponse(reply.getChannel());
        } else if (reply.getRequestType() == NbdRequestType.Write) {
          pydClientManager.markWriteResponse(reply.getChannel());
        } else if (reply.getRequestType() == NbdRequestType.Discard) {
          pydClientManager.markDiscardResponse(reply.getChannel());
        }
      }
    });

  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  public void stop() {

  }
}
