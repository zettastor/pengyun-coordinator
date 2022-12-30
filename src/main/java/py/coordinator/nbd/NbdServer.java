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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.NamedThreadFactory;
import py.common.struct.EndPoint;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.StorageDriver;
import py.driver.DriverType;
import py.icshare.exception.VolumeNotFoundException;
import py.icshare.qos.IoLimitScheduler;
import py.informationcenter.AccessPermissionType;
import py.json.socket.JsonGenerator;
import py.json.socket.JsonSocketServer;
import py.json.socket.JsonSocketServerFactory;
import py.netty.core.IoEventThreadsMode;
import py.netty.core.IoEventUtils;
import py.netty.core.twothreads.TtEventGroup;
import py.netty.core.twothreads.TtServerSocketChannel;


public class NbdServer {

  private static final Logger logger = LoggerFactory.getLogger(NbdServer.class);
  private final StorageDriver storage;
  private ByteBufAllocator allocator;
  private EventLoopGroup bossEventGroup;
  private NbdConfiguration cfg;
  private IoLimitScheduler ioLimitScheduler;
  private ChannelFuture serverChannelFuture;
  private EventLoopGroup workerEventGroup;
  private ThreadPoolExecutor handlerExecutorGroup;
  private PydClientManager pydClientManager;


  
  public NbdServer(NbdConfiguration cfg, StorageDriver storage,
      PydClientManager pydClientManager) {
    this.bossEventGroup = new TtEventGroup(1,
        new DefaultThreadFactory("nbd-server-boss", false, Thread.NORM_PRIORITY));
    this.storage = storage;
    this.cfg = cfg;
    this.pydClientManager = pydClientManager;

    int ioEventGroupThreads = IoEventUtils
        .calculateThreads(IoEventThreadsMode.findByValue(cfg.getNbdServerIoEventGroupThreadsMode()),
            cfg.getNbdServerIoEventGroupThreadsParameter());
    this.workerEventGroup = new TtEventGroup(ioEventGroupThreads,
        new DefaultThreadFactory("nbd-server-io-workers", false, Thread.NORM_PRIORITY));
    int ioEventHandleThreads = IoEventUtils
        .calculateThreads(
            IoEventThreadsMode.findByValue(cfg.getNbdServerIoEventHandleThreadsMode()),
            cfg.getNbdServerIoEventHandleThreadsParameter());
    this.handlerExecutorGroup = new ThreadPoolExecutor(ioEventHandleThreads,
        ioEventHandleThreads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        new NamedThreadFactory("nbd-server-io-handlers"));
  }


  
  @Deprecated
  public static void setPortToFile(int thriftPort) {
    String url = Paths.get(System.getProperty("user.dir"), "config", "coordinator.properties")
        .toString();
    Properties prop = new Properties();
    File file;

    try {
      file = new File(url);
      prop.load(new FileInputStream(file));
      FileOutputStream fos = new FileOutputStream(file);
     
      prop.setProperty("app.main.endpoint", String.valueOf(thriftPort));
      prop.store(fos, "update endpoint");
      fos.close();
    } catch (FileNotFoundException e) {
      logger.error("caught an exception", e);
    } catch (IOException e) {
      logger.error("caught an exception", e);
    }
  }


  
  public void start() throws Exception {
    EndPoint nbdEP = cfg.getEndpoint();

    int maximum = cfg.getNbdMaxBufLengthForAllocateAdapter();
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossEventGroup, this.workerEventGroup).channel(TtServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 256).option(ChannelOption.ALLOCATOR, getAllocator())
        .childOption(ChannelOption.TCP_NODELAY, true).childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.WRITE_SPIN_COUNT, 50)
        .childOption(ChannelOption.ALLOCATOR, getAllocator())
        .childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(512, 8192, maximum))
        .childHandler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {

           
            Map<String, AccessPermissionType> accessRuleTable = cfg.getVolumeAccessRules();
            NbdConnectionHandler connectionHandler = new NbdConnectionHandler();
            connectionHandler.setStorage(storage);
            if (cfg.getDriverType() == DriverType.ISCSI) {
              connectionHandler.setIsolate(true);
            } else {
              connectionHandler.setIsolate(false);
            }
            connectionHandler.setAccessRemoteHostNames(accessRuleTable);
            connectionHandler.setIoLimitScheduler(ioLimitScheduler);
            connectionHandler.setPydClientManager(pydClientManager);
            connectionHandler.setEnableClientConnectionLimit(cfg.isEnableClientConnectionLimit());
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(connectionHandler);

            if (cfg.isEnableHeartbeatHandler()) {
              logger.warn("enable heartbeat handler, read idle time:{} second",
                  cfg.getReaderIdleTimeoutSec());
             
            }

           
            NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder();
            decoder
                .setMaxFrameSize(CoordinatorConfigSingleton.getInstance().getMaxNetworkFrameSize());
            pipeline.addLast(decoder);

           
            pipeline.addLast(newDispatcher());
          }
        });

    this.serverChannelFuture = bootstrap
        .bind(new InetSocketAddress(nbdEP.getHostName(), nbdEP.getPort())).sync();
    logger.warn("Started the nbd server:[{}], nbd server cfg:{}", nbdEP, cfg);
  }


  
  @Deprecated
  public JsonSocketServer newJsonSocketServer() {
    JsonSocketServerFactory jsonSocketServerFactory = new JsonSocketServerFactory();
    JsonGenerator jsonGenerator = new JsonGenerator() {
      @Override
      public String generate() {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> tmpKeyList = null;
        try {
          tmpKeyList = new ArrayList<>();
          for (SocketAddress channelAddress : pydClientManager.getAllClients()) {
            tmpKeyList.add(channelAddress.toString());
          }
          String json = objectMapper.writeValueAsString(tmpKeyList);
          return json;
        } catch (JsonProcessingException e) {
          logger.warn("Caught an exception when generate json string for object {}", tmpKeyList, e);
        }

        return null;
      }
    };

    jsonSocketServerFactory.setJsonGenerator(jsonGenerator);
    jsonSocketServerFactory.setVolumeId(cfg.getVolumeId());
    return jsonSocketServerFactory.createJsonSocketServer();
  }


  
  public NbdRequestFrameDispatcher newDispatcher() throws VolumeNotFoundException {
    NbdResponseSender sender = new NbdResponseSender();
    sender.setPydClientManager(pydClientManager);
    NbdRequestFrameDispatcher frameDispatcher = new NbdRequestFrameDispatcher(storage, sender);

    frameDispatcher.setIoLimitScheduler(ioLimitScheduler);
    frameDispatcher.setReadOnlyBySnapshot(cfg.getSnapshotId() != 0);
    frameDispatcher.setAccessPermissionTable(cfg.getVolumeAccessRules());
    frameDispatcher.setPydClientManager(pydClientManager);
    frameDispatcher.setEventExecutorGroup(handlerExecutorGroup);
    return frameDispatcher;
  }


  
  public void close() {
    try {
      if (bossEventGroup != null) {
        Future<?> future1 = bossEventGroup.shutdownGracefully();
        future1.get(5000, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Caught an exception", e);
    }

    try {
      if (workerEventGroup != null) {
        Future<?> future1 = workerEventGroup.shutdownGracefully();
        future1.get(5000, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.warn("Caught an exception", e);
    }

    handlerExecutorGroup.shutdown();

    try {
      pydClientManager.stop();
    } catch (Exception e) {
      logger.error("caught an exception when stop pyd client manager", e);
    }

    try {
      serverChannelFuture.channel().close();
    } catch (Exception e) {
      logger.warn("Caught an exception", e);
    }
  }

  public IoLimitScheduler getIoLimitScheduler() {
    return ioLimitScheduler;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }

  public ByteBufAllocator getAllocator() {
    return allocator;
  }

  public void setAllocator(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

}

