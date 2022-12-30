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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;


public class NbdReadTimeOutHandler extends ReadTimeoutHandler {

  private static final Logger logger = LoggerFactory.getLogger(NbdReadTimeOutHandler.class);

  private final PydClientManager pydClientManager;

  public NbdReadTimeOutHandler(int timeoutSeconds, PydClientManager pydClientManager) {
    super(timeoutSeconds);
    this.pydClientManager = pydClientManager;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof ReadTimeoutException) {
      logger.warn("channel receive:{} idle", ctx.channel().remoteAddress());
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
    } else {
      logger.warn("channel:{} caught other exception", ctx.channel().remoteAddress());
    }
    super.exceptionCaught(ctx, cause);
  }

}
