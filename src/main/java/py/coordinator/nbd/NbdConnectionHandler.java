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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.Negotiation;
import py.icshare.qos.IoLimitScheduler;
import py.informationcenter.AccessPermissionType;


public class NbdConnectionHandler extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(NbdConnectionHandler.class);
 
 
 
  private StorageDriver storage;

  private Map<String, AccessPermissionType> accessRemoteHostNames;

  private IoLimitScheduler ioLimitScheduler;
  private PydClientManager pydClientManager;
  private boolean enableClientConnectionLimit = true;
 
 
 
  private boolean isolate = false;


  
  public static String parseHostname(SocketAddress socketAddress) {
    String remoteHostname;
    if (socketAddress instanceof InetSocketAddress) {
      remoteHostname = ((InetSocketAddress) socketAddress).getAddress().toString();
     
    } else {
      remoteHostname = socketAddress.toString().split(":")[0];
    }

    if (remoteHostname.startsWith("/")) {
      logger.warn("remote address:{} start with special char", remoteHostname);
      remoteHostname = remoteHostname.substring(1);
    }
    return remoteHostname;
  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channel:{} inactive", ctx.channel().id());
    pydClientManager.clientInactive(ctx.channel());
    if (!pydClientManager.clientExist(ctx.channel())) {
      ioLimitScheduler.close();
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channel:{} unregistered", ctx.channel().id());
    pydClientManager.clientInactive(ctx.channel());
    if (!pydClientManager.clientExist(ctx.channel())) {
      ioLimitScheduler.close();
    }
    ctx.fireChannelUnregistered();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error("channel:{} caught exception", ctx.channel().id(), cause);
    pydClientManager.clientInactive(ctx.channel());
    if (!pydClientManager.clientExist(ctx.channel())) {
      ioLimitScheduler.close();
    }
    ctx.fireExceptionCaught(cause);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
   
   
    SocketAddress socketAddress = ctx.channel().remoteAddress();
    String remoteHostname = parseHostname(socketAddress);

    logger.warn("try to find remote client {} from accessRemoteHostNames {}", remoteHostname,
        accessRemoteHostNames);

    if (!hasAccessToConnectIn(remoteHostname)) {
      logger.warn("remote nbd client on host {} is not allowed to connect in", remoteHostname);
      ctx.channel().close();
      return;
    }

    AccessPermissionType accessPermissionType = getAccessPermissionType(remoteHostname);

    
    if (isEnableClientConnectionLimit()) {
      boolean accept = pydClientManager.limitSingleUser(ctx.channel());
      if (!accept) {
        logger.warn("remote client:{} is not accepted", socketAddress);
        ctx.channel().close();
        return;
      }
    }

    pydClientManager.clientActive(ctx.channel(), accessPermissionType);
    logger.warn("a pyd client:{} is connected, and its' access permission type:{}", socketAddress,
        accessPermissionType);

    try {
      final long startTime = System.currentTimeMillis();

      logger.warn("open io limitation for:{}", socketAddress);
     
      ioLimitScheduler.open();

      logger.warn("judge connection({} to {}) if has right to write", socketAddress,
          ctx.channel().localAddress());

      logger.warn("done this cost time:{}", System.currentTimeMillis() - startTime);
    } catch (Exception e1) {
      logger.error("failed to init storage", e1);
      ctx.channel().close();
      return;
    }

   
    negotiation(ctx.channel());
    logger.warn("a pyd client connection:{} is built", socketAddress);
  }

  private AccessPermissionType getAccessPermissionType(String remoteHostName) {
    if (Utils.isLocalIpAddress(remoteHostName)) {
      return AccessPermissionType.READWRITE;
    }
    AccessPermissionType type = accessRemoteHostNames.get(remoteHostName);
    Validate.notNull(type);
    return type;
  }

  public void setStorage(StorageDriver storage) {
    this.storage = storage;
  }

  public void setAccessRemoteHostNames(Map<String, AccessPermissionType> accessRemoteHostNames) {
    this.accessRemoteHostNames = accessRemoteHostNames;
  }

  public void setIsolate(boolean isolate) {
    this.isolate = isolate;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }

  private void negotiation(Channel channel) {
    int bufferSize = Negotiation.getNegotiateLength();
    ByteBuf byteBuf = Unpooled.buffer(bufferSize);
    Negotiation negotiation = MagicType.generateNegotiation(storage.size());
    negotiation.writeTo(byteBuf);
    channel.writeAndFlush(byteBuf);
  }

  private boolean hasAccessToConnectIn(String remoteHostName) {
   
    if (Utils.isLocalIpAddress(remoteHostName)) {
      return true;
    } else if (isolate) {
     
      return false;
    } else if (accessRemoteHostNames == null || accessRemoteHostNames.size() == 0) {
     
      return false;
    } else if (accessRemoteHostNames.containsKey(remoteHostName)) {
     
      return true;
    } else {
     
      return false;
    }
  }

  public boolean isEnableClientConnectionLimit() {
    return enableClientConnectionLimit;
  }

  public void setEnableClientConnectionLimit(boolean enableClientConnectionLimit) {
    this.enableClientConnectionLimit = enableClientConnectionLimit;
  }
}
