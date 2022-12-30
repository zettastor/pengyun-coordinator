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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.iorequest.iorequest.IoRequestImpl;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.lib.NbdAsyncIoCallBack;
import py.coordinator.lib.StorageDriver;
import py.coordinator.lib.TempResendRequestStore;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.Reply;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.exception.StorageException;
import py.icshare.qos.IoLimitScheduler;
import py.informationcenter.AccessPermissionType;

@Sharable
public class NbdRequestFrameDispatcher extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(NbdRequestFrameDispatcher.class);

  private static AtomicBoolean setIoLimitStatus = new AtomicBoolean(true);
  private final NbdResponseSender sender;
  private final StorageDriver storage;
  private final long lengthForSetIolimit = 512L * 1024;
  Multimap<ChannelId, Pair<ChannelHandlerContext, Request>> pauseRecordMap;
  private Map<String, AccessPermissionType> accessPermissionTable;
  private boolean isReadOnlyBySnapshot = false;
  private IoLimitScheduler ioLimitScheduler;
  private PydClientManager pydClientManager;
  private byte[] bufferForDiscard;
  private AtomicLong requestIdGenerator;
  private ThreadPoolExecutor eventExecutorGroup;
  private volatile boolean channelDisconnect;

  public NbdRequestFrameDispatcher(StorageDriver storage, NbdResponseSender sender) {
    this.storage = storage;
    this.sender = sender;
    this.bufferForDiscard = null;
    requestIdGenerator = new AtomicLong(0);
    this.pauseRecordMap = Multimaps
        .synchronizedSetMultimap(
            HashMultimap.<ChannelId, Pair<ChannelHandlerContext, Request>>create());
    this.channelDisconnect = false;
  }


  public boolean checkPermission(ChannelHandlerContext ctx, Request request) {
    Channel channel = ctx.channel();
    SocketAddress socketAddress = channel.remoteAddress();
    String remoteHosName;
    if (socketAddress instanceof InetSocketAddress) {
      remoteHosName = ((InetSocketAddress) socketAddress).getAddress().toString();
    } else {
      remoteHosName = socketAddress.toString().split(":")[0];
    }

    if (remoteHosName.startsWith("/")) {
      remoteHosName = remoteHosName.substring(1);
    }

    AccessPermissionType permission = null;
    if (accessPermissionTable != null) {
      permission = accessPermissionTable.get(remoteHosName);
    }

    RequestHeader requestHeader = request.getHeader();
    NbdRequestType requestType = requestHeader.getRequestType();

    if (requestType == NbdRequestType.Write) {
      if (isReadOnlyBySnapshot) {
        pydClientManager.markWriteComing(ctx.channel());
        logger.warn("snapshot is just for read and return OK to write request directly");
        Reply reply = Reply
            .generateReply(requestHeader, ProtocoalConstants.SUCCEEDED, null, ctx.channel());
        channel.write(reply.asByteBuf());
        pydClientManager.markWriteResponse(ctx.channel());
        return false;
      }

      if (permission != null && permission == AccessPermissionType.READ) {
        pydClientManager.markWriteComing(ctx.channel());
        logger.error("access permission to the storage is read only");
        Reply reply = Reply
            .generateReply(requestHeader, ProtocoalConstants.EROFS, null, ctx.channel());
        channel.write(reply.asByteBuf());
        pydClientManager.markWriteResponse(ctx.channel());
        return false;
      }
    }
    return true;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channel:{} unregistered", ctx.channel().id());
    this.channelDisconnect = true;
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channel:{} inactive", ctx.channel().id());
    this.channelDisconnect = true;
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error("channel:{} caught exception", ctx.channel().id(), cause);
    this.channelDisconnect = true;
    ctx.fireExceptionCaught(cause);
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    @SuppressWarnings("unchecked") Collection<Request> requests = (Collection<Request>) msg;
    processRequests(ctx, requests);
  }

  public void processRequests(final ChannelHandlerContext ctx, Collection<Request> requests)
      throws Exception {
    if (eventExecutorGroup != null) {
      eventExecutorGroup.execute(new Runnable() {
        @Override
        public void run() {
          try {
            process(ctx, requests);
          } catch (Exception e1) {
            logger.error("caught an exception ", e1);
            ctx.fireExceptionCaught(e1);
          }
        }
      });
    } else {
      try {
        process(ctx, requests);
      } catch (Exception e1) {
        logger.error("caught an exception ", e1);
        throw e1;
      }
    }
  }

  private void process(ChannelHandlerContext ctx, Collection<Request> requests) throws Exception {
    AtomicInteger ioRequestCount = new AtomicInteger(0);
    AtomicLong requestUuid = new AtomicLong(requestIdGenerator.incrementAndGet());

    try {
      List<Request> resendRequests = TempResendRequestStore.pullResendRequests();
      if (!resendRequests.isEmpty()) {
        requests.addAll(resendRequests);
      }

      for (Request request : requests) {
        RequestHeader requestHeader = request.getHeader();
        if (CoordinatorConfigSingleton.getInstance().isShutdown()) {
          logger.warn("coordinator is shutting down, should not reply:{}", requestHeader);
          request.release();
          continue;
        }

        if (channelDisconnect) {
          logger.warn("channel:{} is disconnecting, just release request", ctx.channel().id());
          request.release();
          continue;
        }

        if (ioLimitScheduler != null) {
          ioLimitScheduler.tryThroughput(requestHeader.getLength());
          if (requestHeader.getLength() == lengthForSetIolimit) {
            if (setIoLimitStatus.get()) {
              setIoLimitStatus.set(false);
              ioLimitScheduler.tryGettingIos(requestHeader.getIoSum());
              logger.info("i am going to work for 512KB");
            } else {
              setIoLimitStatus.set(true);
            }
          } else {
            ioLimitScheduler.tryGettingIos(requestHeader.getIoSum());
          }
        }

        if (!checkPermission(ctx, request)) {
          request.release();
          logger.error("new permission: {} for request: {}", accessPermissionTable, request);
          continue;
        }

        NbdRequestType requestType = requestHeader.getRequestType();
        IoRequestImpl ioRequest;
        if (requestType == NbdRequestType.Read) {
          logger.info("receive a read request:{}", requestHeader);
          if (storage.isPause()) {
            ChannelId channelId = ctx.channel().id();
            pauseRecordMap.put(channelId, new Pair<>(ctx, request));
            logger.warn("coordinator, cache read request:{}", requestHeader);
            continue;
          }

          NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(ctx.channel(),
              requestHeader,
              sender);
          ioRequest = new IoRequestImpl(requestHeader.getOffset(), requestHeader.getLength(),
              IoRequestType.Read, null, nbdAsyncIoCallBack, requestHeader.getIoSum());

          if (CoordinatorConfigSingleton.getInstance().getRwLogFlag()) {
            logger.warn("read io coming: {}, from channel:{}", request, ctx.channel());
          } else {
            logger.info("read io coming: {}, from channel:{}", request, ctx.channel());
          }

          pydClientManager.markReadComing(ctx.channel());

          storage.accumulateIoRequest(requestUuid.get(), ioRequest);
          ioRequestCount.incrementAndGet();
          middleProcessTrySemaphoreMaxCountPerTime(ioRequestCount, requestUuid);
        } else if (requestType == NbdRequestType.Write) {
          logger.info("receive a write request:{}", requestHeader);
          if (storage.isPause()) {
            ChannelId channelId = ctx.channel().id();
            pauseRecordMap.put(channelId, new Pair<>(ctx, request));
            logger.warn("coordinator, cache write request:{}", requestHeader);
            continue;
          }

          NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(ctx.channel(),
              requestHeader,
              sender);
          ioRequest = new IoRequestImpl(requestHeader.getOffset(), requestHeader.getLength(),
              IoRequestType.Write, request.getBody(), nbdAsyncIoCallBack, requestHeader.getIoSum());

          request.releaseReference();
          if (CoordinatorConfigSingleton.getInstance().getRwLogFlag()) {
            ByteBuf body = request.getBody();
            byte[] output = new byte[4];
            body.readBytes(output);
            logger
                .warn("write io coming: {}, from channel:{}, four byte:{}", request, ctx.channel(),
                    output);
          } else {
            logger.info("write io coming: {}, from channel:{}", request, ctx.channel());
          }
          pydClientManager.markWriteComing(ctx.channel());

          if (request.getHeader().getLength() != ioRequest.getBody().readableBytes()) {
            Validate.isTrue(false,
                "length=" + request.getHeader().getLength() + ", expected=" + request.getBody()
                    .readableBytes());
          }

          storage.accumulateIoRequest(requestUuid.get(), ioRequest);
          ioRequestCount.incrementAndGet();
          middleProcessTrySemaphoreMaxCountPerTime(ioRequestCount, requestUuid);
        } else if (requestType == NbdRequestType.Discard) {
          if (storage.isPause()) {
            ChannelId channelId = ctx.channel().id();
            pauseRecordMap.put(channelId, new Pair<>(ctx, request));
            continue;
          }

          NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(ctx.channel(),
              requestHeader,
              sender);

          logger.warn("receive a Discard request:{}", requestHeader);
          if (requestHeader.getLength() <= 0) {
            Validate.isTrue(false, "length can not less than zero, " + requestHeader.getLength());
          }

          ioRequest = new IoRequestImpl(requestHeader.getOffset(), requestHeader.getLength(),
              IoRequestType.Discard, null, nbdAsyncIoCallBack, requestHeader.getIoSum());
          pydClientManager.markDiscardComing(ctx.channel());
          storage.accumulateIoRequest(requestUuid.get(), ioRequest);
          ioRequestCount.incrementAndGet();
          middleProcessTrySemaphoreMaxCountPerTime(ioRequestCount, requestUuid);

        } else if (requestType == NbdRequestType.Disc || requestType == NbdRequestType.Flush) {

          logger.error("FLush or Disc, don't do anything yet, request type {}",
              requestHeader.getRequestType());
          Reply reply = Reply
              .generateReply(request.getHeader(), ProtocoalConstants.SUCCEEDED, null,
                  ctx.channel());
          ctx.channel().writeAndFlush(reply.asByteBuf());
        } else if (requestType == NbdRequestType.Heartbeat) {
          logger.info("Receive a heart beat from remote client:{}, nothing to do",
              ctx.channel().remoteAddress().toString());
          pydClientManager.markHeartbeat(ctx.channel());
          Reply reply = Reply.generateHeartbeatReply(request.getHeader(), ctx.channel());
          ctx.channel().writeAndFlush(reply.asByteBuf());
        } else if (requestType == NbdRequestType.ActiveHearbeat) {
          logger.warn("Receive a request of get sending heart beat from remote client:{}.",
              ctx.channel().remoteAddress().toString());
          pydClientManager.markActiveHeartbeat(ctx.channel(), this);
        } else {
          logger.error("don't know how to do with request:{}", request);
          Reply reply = Reply
              .generateReply(requestHeader, ProtocoalConstants.ENOEXEC, null, ctx.channel());
          ctx.channel().writeAndFlush(reply.asByteBuf());
        }
      }
    } finally {
      lastProcessTrySemaphoreMaxCountPerTime(ioRequestCount, requestUuid);
    }
  }

  private void middleProcessTrySemaphoreMaxCountPerTime(AtomicInteger ioRequestCount,
      AtomicLong requestUuid)
      throws StorageException {

    if (ioRequestCount.get() >= CoordinatorConfigSingleton.getInstance()
        .getTrySemaphoreMaxCountPerTime()) {
      getTicketsAndSubmitIoRequests(ioRequestCount, requestUuid);
      ioRequestCount.set(0);
      requestUuid.set(requestIdGenerator.incrementAndGet());
    }
  }

  private void lastProcessTrySemaphoreMaxCountPerTime(AtomicInteger ioRequestCount,
      AtomicLong requestUuid)
      throws StorageException {
    if (ioRequestCount.get() > 0) {
      getTicketsAndSubmitIoRequests(ioRequestCount, requestUuid);
    }
  }

  private void getTicketsAndSubmitIoRequests(AtomicInteger ioRequestCount, AtomicLong requestUuid)
      throws StorageException {
    storage.getTickets(ioRequestCount.get());
    storage.submitIoRequests(requestUuid.get());
  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }

  public void setReadOnlyBySnapshot(boolean isReadOnlyBySnapshot) {
    this.isReadOnlyBySnapshot = isReadOnlyBySnapshot;
  }

  public void setAccessPermissionTable(Map<String, AccessPermissionType> accessPermissionTable) {
    this.accessPermissionTable = accessPermissionTable;
  }

  public void setEventExecutorGroup(ThreadPoolExecutor eventExecutorGroup) {
    this.eventExecutorGroup = eventExecutorGroup;
  }



  public void fireAllCachedRequestWhenRestart() throws Exception {
    if (!pauseRecordMap.isEmpty()) {
      int i = 0;
      for (Collection<Pair<ChannelHandlerContext, Request>> pairCollection : pauseRecordMap.asMap()
          .values()) {
        logger.warn("coordinator, 1, fire cached request:{}", i++);
        if (!pairCollection.isEmpty()) {
          for (Pair<ChannelHandlerContext, Request> pair : pairCollection) {
            Collection<Request> tmpCollection = new ArrayList<>();
            tmpCollection.add(pair.getSecond());
            logger.warn("coordinator, 2, fire cached request:{}", pair.getFirst().channel().id());
            processRequests(pair.getFirst(), tmpCollection);
          }
        }
      }
    }
  }

}
