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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.utils.GetMicrosecondTimestamp;


public class NbdByteToMessageDecoder extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(NbdByteToMessageDecoder.class);
  private MagicType magicType;
  private RequestHeader requestHeader;

  private ByteBuf cumulation;
  private List<Request> requests;
  private int maxFrameSize = 1024 * 1024;
  private long readIndex = 0;
  private long writeIndex = 0;


  
  public NbdByteToMessageDecoder() {
    if (isSharable()) {
      throw new IllegalStateException("@Sharable annotation is not allowed");
    }
    requests = new ArrayList<>();
  }

  private void clear() {
    this.magicType = null;
    this.requestHeader = null;
    this.cumulation = null;
  }

  private void clearWhenChannelDisable() {
    if (cumulation != null) {
      cumulation.release();
      cumulation = null;
    }

    if (!requests.isEmpty()) {
      for (Request request : requests) {
        request.release();
      }
      requests.clear();
    }
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    logger.warn(
        "unregistered, cumulation:{}, magicType:{}, requestHeader:{}, channel:{}, requests:{}, "
            + "isAutoRead:{}",
        cumulation, magicType, requestHeader, ctx.channel(), requests,
        ctx.channel().config().isAutoRead());

    clearWhenChannelDisable();

    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    logger.warn("channel:{} inactive, magicType:{}, requestHeader:{}, requests:{}, isAutoRead:{}",
        ctx.channel(),
        magicType, requestHeader, requests, ctx.channel().config().isAutoRead());

    clearWhenChannelDisable();

    ctx.fireChannelInactive();
  }


  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    logger.error(
        "channel:{} caught exception, magicType:{}, requestHeader:{}, requests:{}, isAutoRead:{}",
        ctx.channel(),
        magicType, requestHeader, requests, ctx.channel().config().isAutoRead(), cause);

    clearWhenChannelDisable();

    ctx.fireExceptionCaught(cause);
  }

  private ByteBuf processCumulation(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
    logger.debug("receive a buffer:{} at channel:{}", data, ctx.channel());
    
    if (magicType == null) {
     
      ByteBuf tmp = Unpooled.wrappedBuffer(cumulation, data);
      int cumulationBytes = cumulation.readableBytes();
      if (tmp.readableBytes() < MagicType.getMagicLength()) {
        cumulation = tmp;
        return null;
      }

      magicType = MagicType.findByValue(tmp.readInt());
      data.skipBytes(MagicType.getMagicLength() - cumulationBytes);
      if (data.isReadable()) {
        data.retain();
      } else {
       
        data = null;
      }

      cumulation = null;
      tmp.release();
      return data;
    }

    if (requestHeader == null) {
     
      int cumulationSize = cumulation.readableBytes();
      ByteBuf tmp = Unpooled.wrappedBuffer(cumulation, data);
      if (tmp.readableBytes() < magicType.getHeaderLength()) {
        cumulation = tmp;
        return null;
      }

      requestHeader = magicType.getRequestHeader(tmp);
      data.skipBytes(magicType.getHeaderLength() - cumulationSize);
      if (data.isReadable()) {
        data.retain();
      } else {
       
        data = null;
      }

      tmp.release();

      if (!requestHeader.getRequestType().isWrite() || (requestHeader.getRequestType().isWrite()
          && requestHeader.getLength() == 0)) {
        if (requestHeader.getRequestType().isRead()) {
          logger.debug("get a read request {}", requestHeader);
        }
        fireChannelRead(requestHeader, null);
      } else {
        cumulation = null;
      }
      return data;
    }

    Validate.isTrue(requestHeader.getRequestType().isWrite());
   
    int length = (int) requestHeader.getLength();
    Validate.isTrue(length > 0);
    if (length <= data.readableBytes() + cumulation.readableBytes()) {
     
      int leftSize = length - cumulation.readableBytes();
      final ByteBuf pack = Unpooled
          .wrappedBuffer(cumulation, data.slice(data.readerIndex(), leftSize));
      data.skipBytes(leftSize);

      if (data.isReadable()) {
       
        data.retain();
      } else {
        data = null;
      }

      logger.debug("get an write request {}", requestHeader);
      fireChannelRead(requestHeader, pack);
      return data;
    } else {
     
     
      cumulation = Unpooled.wrappedBuffer(cumulation, data);
      return null;
    }
  }

  private void processData(ByteBuf data) throws Exception {
    while (data.isReadable()) {
     
      if (magicType == null) {
       
        if (data.readableBytes() < MagicType.getMagicLength()) {
          cumulation = data;
          break;
        }

        magicType = MagicType.findByValue(data.readInt());
        if (!data.isReadable()) {
          data.release();
         
          break;
        }
      }

      if (requestHeader == null) {
       
        if (data.readableBytes() < magicType.getHeaderLength()) {
          cumulation = data;
          break;
        }

        requestHeader = magicType.getRequestHeader(data);

       
        if (!requestHeader.getRequestType().isWrite() || (requestHeader.getRequestType().isWrite()
            && requestHeader.getLength() == 0)) {
          if (requestHeader.getRequestType().isRead()) {
            logger.debug("get a read request {}", requestHeader);
          }
          fireChannelRead(requestHeader, null);
         
          if (!data.isReadable()) {
            data.release();
            break;
          } else {
            continue;
          }
        } else {
          if (!data.isReadable()) {
            data.release();
            break;
          }
        }
      }

      if (!requestHeader.getRequestType().isWrite()) {
        Validate.isTrue(false, "can not happen:" + requestHeader.getRequestType());
      }
     
      int length = (int) requestHeader.getLength();
      Validate.isTrue(length > 0);
      if (length <= data.readableBytes()) {
       
        ByteBuf pack = data.slice(data.readerIndex(), length);
        data.skipBytes(length);

        if (data.isReadable()) {
         
          data.retain();
          fireChannelRead(requestHeader, pack);
        } else {
         
         
          fireChannelRead(requestHeader, pack);
          break;
        }
      } else {
       
       
        cumulation = data;
        break;
      }
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof ByteBuf)) {
      logger.error("it is not bytebuf={}", msg);
      ctx.fireChannelRead(msg);
      return;
    }

    ByteBuf data = ((ByteBuf) msg).order(ByteOrder.BIG_ENDIAN);
    try {
     
      if (cumulation != null) {
        data = processCumulation(ctx, data);
      }

      if (data != null) {
        processData(data);
      }
      if (requests.size() > 0) {
        logger.debug("fire requests: {}", requests);
        List<Request> requestList = new ArrayList<>();
        requestList.addAll(requests);
        ctx.fireChannelRead(requestList);
      }
    } catch (Throwable t) {
      logger.error("nbd decoder caught an exception", t);
      clearWhenChannelDisable();
      if (data != null) {
        data.release();
      }
     
      ctx.fireChannelInactive();
    } finally {
      requests.clear();
    }
  }


  
  public void fireChannelRead(RequestHeader header, ByteBuf body) {
    if (!header.getRequestType().isDiscard() && (header.getLength() > maxFrameSize)) {
     
      Validate.isTrue(false, "get a request that is too larger: " + header.getLength());
      logger.error("get a request that is too larger, request={}, maxFrameSize={}", header,
          maxFrameSize);
      if (body != null) {
        body.release();
      }
      return;
    }

    if (header.getMagicType() == MagicType.PYD_DEBUG) {
      logger.info("request type:{}, and request timestamp:{}", header.getRequestType(),
          header.getNbdClientTimestamp());
      Long currentNanoTime = GetMicrosecondTimestamp.getCurrentTimeMicros();
      Long networkReceiveInterval = currentNanoTime - header.getNbdClientTimestamp();
    }

    if (header.getRequestType().isRead()) {
      if (body != null) {
        logger.error("request header:{} can not has body", header);
        Validate.isTrue(false);
      }
    } else if (header.getRequestType().isWrite()) {
      if (body.readableBytes() != header.getLength()) {
        logger.error("request header:{}, body:{}", header, body);
        Validate.isTrue(false);
      }
    } else if (header.getRequestType().isDiscard()) {
      if (body != null) {
        logger.error("request header:{} can not has body", header);
        Validate.isTrue(false);
      }
    }


    requests.add(new Request(header, body));
    clear();

  }

  public void setMaxFrameSize(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

}
