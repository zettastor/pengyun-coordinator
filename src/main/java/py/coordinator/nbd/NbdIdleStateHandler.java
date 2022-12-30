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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;


public class NbdIdleStateHandler extends ChannelDuplexHandler {

  private static final Logger logger = LoggerFactory.getLogger(NbdIdleStateHandler.class);
  private PydClientManager pydClientManager;

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        logger.warn("Channel receive:{} idle", ctx.channel().remoteAddress());
        boolean close = pydClientManager.checkAndCloseClientConnection(ctx.channel());
        if (close) {
          logger.warn("gonna to close pyd client:{}", ctx.channel().remoteAddress());
          ctx.close();
        }


        for (SocketAddress channelAddress : pydClientManager.getAllClients()) {
          PydClientManager.ClientInfo clientInfo = pydClientManager.getClientInfo(ctx.channel());
          if (clientInfo == null) {
            logger.warn("can not get client info by:{}", channelAddress);
            continue;
          }
          long lastHeartbeatTime = clientInfo.getHeartbeatTime();
          String lastHeartbeatTimeStr = Utils.millsecondToString(lastHeartbeatTime);
          logger.warn("client:{}, last heartbeat time:{}", channelAddress,
              lastHeartbeatTimeStr);
        }
      } else if (e.state() == IdleState.WRITER_IDLE) {
        logger.warn("Channel send:{} idle", ctx.channel().remoteAddress());
      } else if (e.state() == IdleState.ALL_IDLE) {
        logger.warn("Channel receive&send:{} idle", ctx.channel().remoteAddress());
      }
    }
  }

}
