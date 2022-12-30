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

package py.coordinator.iorequest.iorequest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.buffer.PyBuffer;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.lib.AsyncIoCallBack;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.performance.PerformanceManager;


public class IoRequestImpl implements IoRequest {

  private static final Logger logger = LoggerFactory.getLogger(IoRequestImpl.class);

  
  private final AtomicInteger referenceCount;
  private final long startTime;
  private final AtomicBoolean hasRespoNse;
  private final IoRequestType ioRequestType;
  private final long offset;
  private final long length;
  private final int ioSum;
  private List<IoUnit> units;
  private ByteBuf body;

  private AsyncIoCallBack asyncIoCallBack;
  private Semaphore ioDepth;
  private long volumeId;


  
  public IoRequestImpl(long offset, long length, IoRequestType ioRequestType, ByteBuf body,
      AsyncIoCallBack asyncIoCallBack, int ioSum) {
    this.offset = offset;
    this.length = length;
    this.ioRequestType = ioRequestType;
    this.body = body;
    this.ioSum = ioSum;
    this.asyncIoCallBack = asyncIoCallBack;
    this.referenceCount = new AtomicInteger(0);
    this.units = new ArrayList<>();
    this.startTime = System.nanoTime();
    this.hasRespoNse = new AtomicBoolean(false);
  }

  public int decReferenceCount() {
    return this.referenceCount.decrementAndGet();
  }

  public int incReferenceCount() {
    return this.referenceCount.incrementAndGet();
  }

  @Override
  public long getOffset() {
    return this.offset;
  }

  @Override
  public long getLength() {
    return this.length;
  }

  @Override
  public ByteBuf getBody() {
    return body;
  }

  @Override
  public int getIoSum() {
    return ioSum;
  }

  @Override
  public void releaseBody() {
    if (body != null) {
      body.release();
      body = null;
    }
  }

  @Override
  public IoRequestType getIoRequestType() {
    return ioRequestType;
  }

  private String buildIoRequestInfo() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("IO request type:[");
    stringBuilder.append(this.ioRequestType);
    stringBuilder.append("], offset:[");
    stringBuilder.append(this.offset);
    stringBuilder.append("], length:[");
    stringBuilder.append(this.length);
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

  @Override
  public void reply() {
    boolean failure = false;
    ByteBuf sendBuf = null;
    CompositeByteBuf compositeByteBuf = null;
    int numUnits = units.size();
    Validate.isTrue(numUnits > 0);

    for (IoUnit ioUnit : units) {
      if (!ioUnit.isSuccess()) {
        logger.warn("fail to request {}", buildIoRequestInfo());
        failure = true;
        Validate.isTrue(ioUnit.getPyBuffer() == null);
        continue;
      }

      if (!ioRequestType.isRead()) {
        Validate.isTrue(ioUnit.getPyBuffer() == null);
        continue;
      }

      PyBuffer readUnitData = ioUnit.getPyBuffer();
      Validate.notNull(readUnitData);
      ByteBuf byteBuf = readUnitData.getByteBuf();
      Validate.isTrue(ioUnit.getLength() == byteBuf.readableBytes());
      if (sendBuf == null) {
        sendBuf = byteBuf;
      } else {
        if (compositeByteBuf == null) {
          compositeByteBuf = new CompositeByteBuf(PooledByteBufAllocatorWrapper.INSTANCE, true,
              numUnits);
          compositeByteBuf.addComponent(true, sendBuf);
        }
        compositeByteBuf.addComponent(true, byteBuf);
      }
    }

   
    if (compositeByteBuf != null) {
      sendBuf = compositeByteBuf;
    }

    long costTime = System.nanoTime() - startTime;
    if (failure) {
      logger.error("fail to respoNse to request:{}, cost time:{}, units:{}, buffer:{}",
          buildIoRequestInfo(),
          costTime, units, sendBuf);
      if (sendBuf != null) {
        sendBuf.release();
        sendBuf = null;
      }
    }

    if (ioRequestType.isWrite()) {
      PerformanceManager.getInstance().getPerformanceManagerMap().get(volumeId)
          .addWriteLatencyNs(costTime);
    } else if (ioRequestType.isRead()) {
      PerformanceManager.getInstance().getPerformanceManagerMap().get(volumeId)
          .addReadLatencyNs(costTime);
      if (!failure) {
        if (sendBuf.readableBytes() != getLength()) {
          Validate.isTrue(false, "read:" + sendBuf + ", request:" + buildIoRequestInfo());
        }
      }
    }

    if (hasRespoNse.getAndSet(true)) {
      logger.error("respoNse the request:{} twice", buildIoRequestInfo());
      return;
    }

    asyncIoCallBack.ioRequestDone(volumeId, failure, sendBuf);

    releaseTicket();

    releaseReference(ioRequestType.isRead());
  }

  @Override
  public void add(IoUnit ioUnit) {
    units.add(ioUnit);
  }

  @Override
  public List<IoUnit> getIoUnits() {
    return units;
  }

  @Override
  public void getTicket(Semaphore ioDepth) {
    setTicketForRelease(ioDepth);
    try {
      this.ioDepth.acquire();
    } catch (InterruptedException e) {
      logger.error("can not get ticket to IO", e);
      throw new RuntimeException();
    }
  }

  @Override
  public void setTicketForRelease(Semaphore ioDepth) {
    this.ioDepth = ioDepth;
  }

  @Override
  public Channel getChannel() {
    return asyncIoCallBack.getChannel();
  }

  @Override
  public long getVolumeId() {
    return volumeId;
  }

  @Override
  public void setVolumeId(Long volumeId) {
    this.volumeId = volumeId;
  }

  private void releaseReference(boolean isRead) {
    if (units != null) {
      if (isRead) {
        if (!units.isEmpty()) {
          for (IoUnit ioUnit : units) {
            ioUnit.releaseReference();
          }
        }
      }
      units.clear();
      units = null;
    }
    asyncIoCallBack = null;
  }

  private void releaseTicket() {
    Validate.notNull(this.ioDepth);
    this.ioDepth.release();
  }

  @Override
  public int getReferenceCount() {
    return referenceCount.get();
  }

  @Override
  public String toString() {
    return "IORequestImpl [request=" + buildIoRequestInfo() + ", referenceCount=" + referenceCount
        + "]";
  }
}
