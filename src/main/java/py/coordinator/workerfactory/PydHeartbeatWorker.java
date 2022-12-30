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

package py.coordinator.workerfactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.NbdRequestFrameDispatcher;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.nbd.request.Reply;
import py.periodic.Worker;


public class PydHeartbeatWorker implements Worker, HeartbeatWorker {

  private static final Logger logger = LoggerFactory.getLogger(PydHeartbeatWorker.class);
  private static final int MAX_WAIT_NO_IO_SECOND = 3;
  private static int noIORequestCount;
  private static AtomicBoolean firstSendStartOnlineMigrationRequest = new AtomicBoolean(false);
  protected PydClientManager pydClientManager;
  private StorageDriver storageDriver;



  public PydHeartbeatWorker(PydClientManager pydClientManager, StorageDriver storageDriver) {
    Validate.notNull(pydClientManager);
    this.storageDriver = storageDriver;
    this.pydClientManager = pydClientManager;
  }

  @Override
  public void doWork() throws Exception {
    doHeartbeatWork();
  }

  @Override
  public void doHeartbeatWork() {
    long startTimeMs = System.currentTimeMillis();
    Iterator<Map.Entry<SocketAddress, PydClientManager.ClientInfo>> iterator = pydClientManager
        .getClientInfoMap()
        .entrySet().iterator();
    boolean needFireAllCachedRequest = false;
    if (storageDriver.isPause() && !storageDriver.hasIoRequest()) {
      noIORequestCount++;
      logger.warn("coordinator, no io request count:{}", noIORequestCount);
      if (noIORequestCount >= MAX_WAIT_NO_IO_SECOND) {
        if (!firstSendStartOnlineMigrationRequest.get()) {
          storageDriver.sendStartOnlineMigrationRequestToAllDatanode();
          firstSendStartOnlineMigrationRequest.set(true);
        } else {
          if (storageDriver.checkAllDatanodeNotifySuccessfullyOrResend()) {
            needFireAllCachedRequest = true;
            noIORequestCount = 0;
            storageDriver.restart();
            firstSendStartOnlineMigrationRequest.set(false);
          }
        }
      }
    } else {
      noIORequestCount = 0;
    }

    while (iterator.hasNext()) {
      Map.Entry<SocketAddress, PydClientManager.ClientInfo> entry = iterator.next();
      PydClientManager.ClientInfo clientInfo = entry.getValue();
      if (clientInfo == null) {
        continue;
      }

      if (needFireAllCachedRequest) {
        NbdRequestFrameDispatcher dispatcher = clientInfo.getDispatcher();
        try {
          dispatcher.fireAllCachedRequestWhenRestart();
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }


      if (!clientInfo.getActiveHeartbeat()) {
        logger.warn("need not send heartbeat to client:{}", clientInfo);
        continue;
      }

      logger.info("send heartbeat to client:{}", clientInfo);
      final long currentTimeMs = System.currentTimeMillis();

      channelHeartbeatWork(clientInfo, currentTimeMs);

      Channel channel = clientInfo.getClient();
      long lastActiveTimeMs = clientInfo.getLastActiveTime();
      long idleTimeMs = currentTimeMs - lastActiveTimeMs;
      long channelIdleTimeoutMs = pydClientManager.getChannelIdleTimeoutSecond() * 1000L;
      if (idleTimeMs >= channelIdleTimeoutMs) {
        logger.warn(
            "pyd client:{} last active time:{}, idle time:{} larger than channel idle timeout:{},"
                + " need to close it",
            channel.remoteAddress(), Utils.millsecondToString(lastActiveTimeMs), idleTimeMs,
            channelIdleTimeoutMs);
        boolean close = pydClientManager.checkAndCloseClientConnection(channel);
        if (close) {
          logger.warn("gonna to close pyd client:{}", channel.remoteAddress());
          channel.close();
        }
      }

    }

    long costTimeMs = System.currentTimeMillis() - startTimeMs;
    if (costTimeMs > PydClientManager.HEARTBEAT_TIME_INTERVAL) {
      logger.error("can not happen, heartbeat:{} count cost time:{}ms",
          pydClientManager.getClientInfoMap().keySet(), costTimeMs);
    }
  }

  @Override
  public void heartbeatIfNeed(Channel channel) {
    if (channel == null) {
      return;
    }
    PydClientManager.ClientInfo clientInfo = pydClientManager.getClientInfo(channel);
    if (clientInfo == null) {
      return;
    }
    channelHeartbeatWork(clientInfo, System.currentTimeMillis());
  }

  private void channelHeartbeatWork(PydClientManager.ClientInfo clientInfo, long currentTimeMs) {
    Validate.notNull(clientInfo);
    Channel channel = clientInfo.getClient();
    Validate.notNull(channel);

    long lastIoTimeMs = clientInfo.getIoTime();
    long passedIoTimeMs = currentTimeMs - lastIoTimeMs;
    if (passedIoTimeMs >= pydClientManager.getTimeIntervalAfterIoRequestMs()) {
      Reply reply = Reply.generateHeartbeatReply(null, channel);
      channel.writeAndFlush(reply.asByteBuf()).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          String timestamp = Utils.millsecondToString(currentTimeMs);
          if (!future.isDone()) {
            logger.error("pyd client:{} future not done at:{}", channel.remoteAddress().toString(),
                timestamp);
          }
          if (future.isCancelled()) {
            logger.error("pyd client:{} connection attempt cancelled by user at:{}",
                channel.remoteAddress().toString(), timestamp);
          } else if (!future.isSuccess()) {
            logger.error("fail to heartbeat:{} to pyd client:{} at:{}",
                channel.remoteAddress().toString(),
                future.channel().remoteAddress().toString(), timestamp, future.cause());
          }
        }
      });
      logger.debug("pyd client:{} last IO time:{}, need heartbeat:{}", channel.remoteAddress(),
          Utils.millsecondToString(lastIoTimeMs), reply);
    } else {
      logger
          .info("pyd client:{} last IO time:{}, timeIntervalAfterIORequestMs:{} no need heartbeat",
              channel.remoteAddress(), Utils.millsecondToString(lastIoTimeMs),
              pydClientManager.getTimeIntervalAfterIoRequestMs());
    }
  }

}
