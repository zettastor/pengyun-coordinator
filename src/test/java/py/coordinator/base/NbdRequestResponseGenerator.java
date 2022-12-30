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

package py.coordinator.base;

import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.icshare.BroadcastLogStatus;
import py.membership.SegmentMembership;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponseUnit;

/**
 * xx.
 */
public class NbdRequestResponseGenerator {

  private static final Logger logger = LoggerFactory.getLogger(NbdRequestResponseGenerator.class);

  private static Random random = new Random();

  public static byte[] generateReadRequestPlan(long offset, int length) {
    return generateReadRequestPlan(0, offset, length);
  }

  /**
   * xx.
   */
  public static byte[] generateReadRequestPlan(long requestId, long offset, int length) {
    MagicType magicType = MagicType.PYD_NORMAL;
    // because we call JNI in code when magicType is MagicType.PYD_DEBUG

    byte[] request = new byte[magicType.getRequestLength()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(request);
    byteBuffer.putInt(magicType.getValue());
    byteBuffer.putInt(NbdRequestType.Read.getValue());
    ByteBuffer handler = ByteBuffer.wrap(new byte[8]);
    handler.putLong(requestId);
    byteBuffer.put(handler.array());
    byteBuffer.putLong(offset);
    byteBuffer.putInt(length);
    byteBuffer.putInt(1);
    if (MagicType.PYD_DEBUG == magicType) {
      byteBuffer.putLong(System.currentTimeMillis());
      byteBuffer.putLong(0);
    }

    return request;
  }


  /**
   * xx.
   */
  public static Request generateReadRequest(long requestId, long offset, int length) {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Read, requestId, offset,
        length, 1);
    return new Request(requestHeader, null);
  }

  public static byte[] generateWriteRequestPlan(long offset, int length) {
    return generateWriteRequestPlan(0, offset, length);
  }

  public static byte[] generateWriteRequestPlan(long offset, int length, byte[] data) {
    return generateWriteRequestPlan(0, offset, length, data);
  }

  public static byte[] generateWriteRequestPlan(long requestId, long offset, int length) {
    return generateWriteRequestPlan(0, offset, length, new byte[length]);
  }


  /**
   * xx.
   */
  public static byte[] generateWriteRequestPlan(long requestId, long offset, int length,
      byte[] data) {
    MagicType magicType = MagicType.PYD_NORMAL;
    // because we call JNI in code when magicType is MagicType.PYD_DEBUG

    Validate.isTrue(data.length == length);
    byte[] request = new byte[magicType.getRequestLength() + data.length];
    ByteBuffer byteBuffer = ByteBuffer.wrap(request);
    byteBuffer.putInt(magicType.getValue());
    byteBuffer.putInt(NbdRequestType.Write.getValue());

    ByteBuffer handler = ByteBuffer.wrap(new byte[8]);
    handler.putLong(requestId);
    byteBuffer.put(handler.array());

    byteBuffer.putLong(offset);
    byteBuffer.putInt(length);
    byteBuffer.putInt(1);
    if (MagicType.PYD_DEBUG == magicType) {
      byteBuffer.putLong(System.currentTimeMillis());
      byteBuffer.putLong(0);
    }

    byteBuffer.put(data);
    return request;
  }


  /**
   * xx.
   */
  public static Request generateWriteRequest(long requestId, long offset, int length, byte[] data) {
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Write, requestId,
        offset, length, 1);
    return new Request(requestHeader, Unpooled.wrappedBuffer(data));
  }

  public static PbWriteResponse generateWriteResponse(long requestId,
      LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList,
      List<WriteIoContextManager> managers) {
    return generateWriteResponse(requestId, ioContextToResultPairList, managers, null);
  }


  /**
   * xx.
   */
  public static PbWriteResponse generateWriteResponse(long requestId,
      LinkedList<Pair<PbWriteRequestUnit, PbIoUnitResult>> ioContextToResultPairList,
      List<WriteIoContextManager> managers, SegmentMembership membership) {
    PbWriteResponse.Builder responseBuilder = PbWriteResponse.newBuilder();
    responseBuilder.setRequestId(requestId);
    List<PbWriteResponseUnit> responseUnits = new ArrayList<PbWriteResponseUnit>();
    for (Pair<PbWriteRequestUnit, PbIoUnitResult> entry : ioContextToResultPairList) {
      responseUnits
          .add(PbRequestResponseHelper
              .buildPbWriteResponseUnitFrom(entry.getFirst(), entry.getSecond()));
    }
    responseBuilder.addAllResponseUnits(responseUnits);
    if (membership != null) {
      responseBuilder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));
    }

    if (managers == null) {
      return responseBuilder.build();
    }

    List<PbBroadcastLogManager> broadcastManagers = new ArrayList<PbBroadcastLogManager>();
    List<PbBroadcastLog> broadcastLogs = new ArrayList<PbBroadcastLog>();
    for (WriteIoContextManager manager : managers) {
      PbBroadcastLogManager.Builder managerBuilder = PbBroadcastLogManager.newBuilder();
      managerBuilder.setRequestId(manager.getRequestId());
      for (BroadcastLog broadcastLog : manager.getLogsToCommit()) {
        BroadcastLog cloneLog = broadcastLog.clone();
        if (broadcastLog.getStatus() == BroadcastLogStatus.Created) {
          cloneLog.setStatus(BroadcastLogStatus.Committed);
        } else if (broadcastLog.getStatus() == BroadcastLogStatus.Abort) {
          cloneLog.setStatus(BroadcastLogStatus.AbortConfirmed);
        }
        broadcastLogs.add(PbRequestResponsePbHelper.buildPbBroadcastLogFrom(cloneLog));
      }

      managerBuilder.addAllBroadcastLogs(broadcastLogs);
      broadcastManagers.add(managerBuilder.build());
      broadcastLogs.clear();
    }
    responseBuilder.addAllLogManagersToCommit(broadcastManagers);
    responseBuilder.addAllResponseUnits(responseUnits);
    return responseBuilder.build();
  }


  /**
   * xx.
   */
  public static PyReadResponse generateReadResponse(long requestId, EndPoint endPoint,
      Pair<PbReadResponseUnit, ByteBuf>[] units) {
    PbReadResponse.Builder responseBuilder = PbReadResponse.newBuilder();
    List<PbReadResponseUnit> responseUnits = new ArrayList<PbReadResponseUnit>();
    responseBuilder.setRequestId(requestId);
    ByteBuf bufferToSend = null;
    for (int i = 0; i < units.length; i++) {
      responseUnits.add(units[i].getFirst());
      if (units[i].getSecond() != null) {
        ByteBuf data = units[i].getSecond();
        Validate.isTrue(units[i].getFirst().getResult() == PbIoUnitResult.OK);
        bufferToSend = bufferToSend == null ? data : Unpooled.wrappedBuffer(bufferToSend, data);
      } else {
        Validate.isTrue(units[i].getFirst().getResult() != PbIoUnitResult.OK);
      }
    }

    responseBuilder.addAllResponseUnits(responseUnits);
    return new PyReadResponse(responseBuilder.build(), bufferToSend);
  }


  /**
   * xx.
   */
  public static ByteBuf getByteBuf(int length, int value) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) value;
    }
    return Unpooled.wrappedBuffer(data);
  }


  /**
   * xx.
   */
  public static byte[] getBuffer(int length, int value) {
    byte[] buffer = new byte[length];
    for (int i = 0; i < length; i++) {
      buffer[i] = (byte) value;
    }
    return buffer;
  }


  /**
   * xx.
   */
  public static void checkBuffer(List<IoUnitContext> contexts, int value) {
    IoUnit ioUnit = null;
    for (IoUnitContext ioContext : contexts) {
      ioUnit = ioContext.getIoUnit();
      ByteBuffer buffer = ioUnit.getPyBuffer().getByteBuffer();
      checkBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(), ioUnit.getLength(),
          value);
    }
  }

  public static void checkBuffer(byte[] data, int value) {
    checkBuffer(data, 0, data.length, value);
  }


  /**
   * xx.
   */
  public static void checkBuffer(byte[] data, int offset, int length, int value) {
    for (int i = 0; i < length; i++) {
      if (data[i + offset] != (byte) value) {
        logger.error("value: {}, expected: {}", data[i], value);
        assertTrue(false);
      }
    }
  }

  public static void checkBuffer(ByteBuf byteBuf, int value) {
    checkBuffer(byteBuf, 0, byteBuf.readableBytes(), value);
  }


  /**
   * xx.
   */
  public static void checkBuffer(ByteBuf byteBuf, int offset, int length, int value) {
    for (int i = 0; i < length; i++) {
      if (byteBuf.getByte(byteBuf.readerIndex() + offset) != (byte) value) {
        logger.error("value: {}, expected: {}", byteBuf.getByte(byteBuf.readerIndex() + offset),
            value);
        assertTrue(false);
      }
    }
  }


  /**
   * xx.
   */
  public static void checkBuffer(ByteBuffer byteBuf, int value) {
    for (int i = byteBuf.arrayOffset() + byteBuf.position(); i < byteBuf.remaining(); i++) {
      if (byteBuf.get(i) != (byte) value) {
        logger.error("value: {}, expected: {}", byteBuf.get(i), value);
        assertTrue(false);
      }
    }
  }
}
