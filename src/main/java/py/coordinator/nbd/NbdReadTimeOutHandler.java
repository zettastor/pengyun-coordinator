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
