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
