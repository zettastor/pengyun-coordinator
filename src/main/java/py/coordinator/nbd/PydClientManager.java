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

import io.netty.channel.Channel;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.coordinator.lib.StorageDriver;
import py.coordinator.workerfactory.PydHeartbeatWorkerFactory;
import py.informationcenter.AccessPermissionType;
import py.periodic.PeriodicWorkExecutor;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;


public class PydClientManager {

  public static final int HEARTBEAT_TIME_INTERVAL = 1000;
  private static final Logger logger = LoggerFactory.getLogger(PydClientManager.class);
  private final int timeIntervalAfterIoRequestMs;
  private final Map<SocketAddress, ClientInfo> clientInfoMap;
  private int channelIdleTimeoutSecond;
  private PeriodicWorkExecutor pydHeartbeatExecutor;


  
  public PydClientManager(int timeIntervalAfterIoRequestMs, boolean enableHeartbeat,
      int channelIdleTimeoutSecond,
      StorageDriver storageDriver) {
    this.timeIntervalAfterIoRequestMs = timeIntervalAfterIoRequestMs;
    this.channelIdleTimeoutSecond = channelIdleTimeoutSecond;
    this.clientInfoMap = new ConcurrentHashMap<>();
    if (enableHeartbeat) {
      PydHeartbeatWorkerFactory pydHeartbeatWorkerFactory = new PydHeartbeatWorkerFactory(this,
          storageDriver);
      pydHeartbeatExecutor = new PeriodicWorkExecutorImpl(
          new ExecutionOptionsReader(1, 1, HEARTBEAT_TIME_INTERVAL, null),
          pydHeartbeatWorkerFactory);
     
      try {
        pydHeartbeatExecutor.start();
      } catch (UnableToStartException e) {
        logger.error("caught an exception start heartbeat worker", e);
        throw new RuntimeException();
      }
      logger.warn("start heartbeat process with pyd clients");
    }
  }


  
  public void stop() {
    if (pydHeartbeatExecutor != null) {
      pydHeartbeatExecutor.stop();
    }
  }


  
  public void markHeartbeat(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markHeartbeatTime();
      logger.info("mark client:{} heartbeat", channel.remoteAddress());
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }

  }


  
  public void markReadComing(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markReadTime();
      clientInfo.markReadComing();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markReadResponse(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markReadResponse();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markWriteComing(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markWriteTime();
      clientInfo.markWriteComing();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markWriteResponse(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markWriteResponse();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markDiscardComing(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markDiscardTime();
      clientInfo.markDiscardComing();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markDiscardResponse(Channel channel) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markDiscardResponse();
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void markActiveHeartbeat(Channel channel, NbdRequestFrameDispatcher dispatcher) {
    Validate.notNull(channel);
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      clientInfo.markActiveHeartbeat(dispatcher);
      logger.warn("mark client:{} activeHeartbeat.", channel.remoteAddress());
    } else {
      logger.error("can not find any client info with:{}", channel.remoteAddress());
    }
  }


  
  public void clientActive(Channel channel, AccessPermissionType accessPermissionType) {
    Validate.notNull(channel);
    if (clientInfoMap.containsKey(channel.remoteAddress())) {
      logger.error("last pyd client:{} is not disconnected, the same one is coming",
          channel.remoteAddress());
    } else {
      ClientInfo clientInfo = new ClientInfo(channel);
      clientInfo.markAccessPermissionTypeWhenClientConnect(accessPermissionType);
      clientInfoMap.put(channel.remoteAddress(), clientInfo);
    }
    logger.warn("a pyd client:{} has:{} right connect and mark it", channel.remoteAddress(),
        accessPermissionType);
  }

  public void clientInactive(Channel channel) {
    Validate.notNull(channel);
    clientInactive(channel.remoteAddress());
  }

  private void clientInactive(SocketAddress socketAddress) {
    Validate.notNull(socketAddress);
    logger.warn("pyd client:{} inactive", socketAddress);
    ClientInfo clientInfo = clientInfoMap.remove(socketAddress);
    if (clientInfo == null) {
      logger.warn("a pyd client:{} had been disconnected before", socketAddress);
    } else {
      clientInfo.getClient().close();
      logger.warn("a pyd client:{} is disconnecting. reset the state", socketAddress);
    }
  }


  
  public boolean limitSingleUser(Channel newChannel) {
    Validate.notNull(newChannel);
    logger.warn("pyd client:{} is connecting, try limit single user", newChannel.remoteAddress());

    if (clientInfoMap.isEmpty()) {
     
      return true;
    } else {
      if (clientInfoMap.size() > 1) {
        Validate.isTrue(false, "clients info:{}", clientInfoMap.keySet());
      }
      Iterator<Map.Entry<SocketAddress, ClientInfo>> iterator = clientInfoMap.entrySet().iterator();
      Map.Entry<SocketAddress, ClientInfo> currentClientInfo;
      if (iterator.hasNext()) {
        currentClientInfo = iterator.next();
      } else {
       
        return true;
      }

      String newRemoteHostname = NbdConnectionHandler.parseHostname(newChannel.remoteAddress());
      Validate.notNull(newRemoteHostname);
      String currentRemoteHostname = NbdConnectionHandler.parseHostname(currentClientInfo.getKey());
      Validate.notNull(currentRemoteHostname);
      logger.warn("new remote hostname:{}, current remote hostname:{}", newRemoteHostname,
          currentRemoteHostname);
      if (newRemoteHostname.equals(currentRemoteHostname)) {
        
        logger.warn("pyd-client:{} reconnect", newChannel.remoteAddress());
        clientInactive(currentClientInfo.getKey());
        return true;
      } else {
        
        logger.warn(
            "refuse new remote address:{} connection, cause current remote address:{} still online",
            newRemoteHostname, currentRemoteHostname);
        return false;
      }
    }
  }


  
  public boolean checkAndCloseClientConnection(Channel channel) {
    Validate.notNull(channel);
    boolean remove = false;
    ClientInfo clientInfo = clientInfoMap.get(channel.remoteAddress());
    if (clientInfo != null) {
      long lastActiveTime = clientInfo.getLastActiveTime();
      String lastActiveTimeStr = Utils.millsecondToString(lastActiveTime);

      long readCount = clientInfo.getReadCount();
      long writeCount = clientInfo.getWriteCount();
      long discardCount = clientInfo.getDiscardCount();
      logger.info(
          "pyd client:{} last active time:{}, current read count:{}, write count:{}, discard "
              + "count:{}",
          channel.remoteAddress(), lastActiveTimeStr, readCount, writeCount, discardCount);
      Validate.isTrue(readCount >= 0);
      Validate.isTrue(writeCount >= 0);
      Validate.isTrue(discardCount >= 0);
      long totalCount = readCount + writeCount + discardCount;
      Validate.isTrue(totalCount >= 0);
      if (totalCount > 0) {
        logger.error("pyd client:{} still has read:{} and write:{}  and discard :{}not response",
            channel.remoteAddress(), readCount, writeCount, discardCount);
      } else {
        logger.warn("pyd client:{} can be closed", channel.remoteAddress());
        remove = true;
      }
    }
    if (remove) {
      logger.warn("need to remove pyd client:{}", channel.remoteAddress());
      clientInfoMap.remove(channel.remoteAddress());
    }
    return remove;
  }

  public Set<SocketAddress> getAllClients() {
    return clientInfoMap.keySet();
  }

  public boolean clientExist(Channel channel) {
    return clientInfoMap.containsKey(channel.remoteAddress());
  }

  public ClientInfo getClientInfo(Channel channel) {
    return clientInfoMap.get(channel.remoteAddress());
  }


  
  public void closeAllClientsForExtendVolume() {
    Collection<ClientInfo> allClients = clientInfoMap.values();
    for (ClientInfo clientInfo : allClients) {
      logger.warn("close pyd client:{} for extend volume", clientInfo.getClient().remoteAddress());
      clientInfo.getClient().close();
    }
    clientInfoMap.clear();
  }

  public Map<SocketAddress, ClientInfo> getClientInfoMap() {
    return clientInfoMap;
  }

  public int getChannelIdleTimeoutSecond() {
    return channelIdleTimeoutSecond;
  }

  public int getTimeIntervalAfterIoRequestMs() {
    return timeIntervalAfterIoRequestMs;
  }

  public class ClientInfo implements Comparable {

    private final Channel client;
    private final AtomicLong heartbeatTime;
    private final AtomicLong readTime;
    private final AtomicLong writeTime;
    private final AtomicLong discardTime;
    private final AtomicLong readCount;
    private final AtomicLong writeCount;
    private final AtomicLong discardCount;
    private final AtomicBoolean activeHeartbeat;
    private final AtomicReference<AccessPermissionType> accessPermissionType;
    private NbdRequestFrameDispatcher dispatcher;


    
    public ClientInfo(Channel channel) {
      this.client = channel;
      this.heartbeatTime = new AtomicLong(0);
      this.readTime = new AtomicLong(0);
      this.writeTime = new AtomicLong(0);
      this.readCount = new AtomicLong(0);
      this.writeCount = new AtomicLong(0);
      this.discardTime = new AtomicLong(0);
      this.discardCount = new AtomicLong(0);
      this.activeHeartbeat = new AtomicBoolean(false); 
      this.accessPermissionType = new AtomicReference<>();
    }

    public Channel getClient() {
      return client;
    }

    public NbdRequestFrameDispatcher getDispatcher() {
      return dispatcher;
    }

    public void markHeartbeatTime() {
      this.heartbeatTime.set(System.currentTimeMillis());
    }

    public void markReadTime() {
      this.readTime.set(System.currentTimeMillis());
    }

    public void markWriteTime() {
      this.writeTime.set(System.currentTimeMillis());
    }

    public void markDiscardTime() {
      this.discardTime.set(System.currentTimeMillis());
    }

    public void markReadComing() {
      this.readCount.incrementAndGet();
    }

    public void markReadResponse() {
      this.readCount.decrementAndGet();
    }

    public void markWriteComing() {
      this.writeCount.incrementAndGet();
    }

    public void markWriteResponse() {
      this.writeCount.decrementAndGet();
    }

    public void markDiscardComing() {
      this.discardCount.incrementAndGet();
    }

    public void markDiscardResponse() {
      this.discardCount.decrementAndGet();
    }

    public void markActiveHeartbeat(NbdRequestFrameDispatcher dispatcher) {
      this.activeHeartbeat.set(true);
      this.dispatcher = dispatcher;
    }


    
    public void markAccessPermissionTypeWhenClientConnect(
        AccessPermissionType accessPermissionType) {
      this.accessPermissionType.compareAndSet(null, accessPermissionType);
      
      markReadTime();
    }

    @Override
    public int compareTo(Object o) {
      if (o == null) {
        return 1;
      }
      Channel other = (Channel) o;

      return client.compareTo(other);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ClientInfo)) {
        return false;
      }

      ClientInfo that = (ClientInfo) o;

      return client != null ? client.remoteAddress().equals(that.client.remoteAddress())
          : that.client == null;
    }

    @Override
    public int hashCode() {
      return client != null ? client.hashCode() : 0;
    }

    public Long getHeartbeatTime() {
      return heartbeatTime.get();
    }

    public boolean getActiveHeartbeat() {
      return activeHeartbeat.get();
    }

    public Long getReadTime() {
      return readTime.get();
    }

    public Long getWriteTime() {
      return writeTime.get();
    }

    public Long getReadCount() {
      return readCount.get();
    }

    public Long getWriteCount() {
      return writeCount.get();
    }

    public Long getDiscardTime() {
      return discardTime.get();
    }

    public Long getDiscardCount() {
      return discardCount.get();
    }

    public Long getIoTime() {
      return Math.max(Math.max(getReadTime(), getWriteTime()), getDiscardTime());
    }

    public Long getLastActiveTime() {
      return Math.max(getIoTime(), getHeartbeatTime());
    }

    public AccessPermissionType getAccessPermissionType() {
      return accessPermissionType.get();
    }

    @Override
    public String toString() {
      return "ClientInfo{" + "client=" + client + ", heartbeatTime=" + heartbeatTime + ", readTime="
          + readTime
          + ", writeTime=" + writeTime + ", readCount=" + readCount + ", writeCount=" + writeCount
          + ", accessPermissionType=" + accessPermissionType + '}';
    }
  }
}
