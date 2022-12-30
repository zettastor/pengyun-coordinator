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

package py.coordinator.main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import py.PbRequestResponseHelper;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.PbWriteResponseUnit;
import py.test.TestBase;
import py.test.TestUtils;

/**
 * xx.
 */
public class ProtocolBufferTest extends TestBase {

  public ProtocolBufferTest() throws Exception {
    super.init();
  }

  @Test
  public void testUpdateListObject() {
    PbMembership membership = PbRequestResponseHelper
        .buildPbMembershipFrom(TestUtils.generateMembership());
    PbMembership.Builder builder = PbMembership.newBuilder(membership);
    List<Long> tmp = new ArrayList<Long>();
    tmp.addAll(builder.getSecondariesList());
    tmp.remove(0);
    builder.clearSecondaries();
    builder.addAllSecondaries(tmp);
    PbMembership membership1 = builder.build();
    assertTrue(membership1.getSecondariesCount() == 1);
  }

  @Test
  public void testByteString() {
    ByteString byteString = ByteString.copyFrom(new byte[1024]);
    ByteBuffer byteBuffer = byteString.asReadOnlyByteBuffer();
    assertFalse(byteBuffer.hasArray());
    try {
      byteBuffer.array();
    } catch (Exception e) {
      assertTrue(e instanceof ReadOnlyBufferException);
    }
    logger.info("byte buffer: {}", byteBuffer);
    assertTrue(byteBuffer.remaining() == 1024);

    byteString = byteString.substring(10, 100);
    byteBuffer = byteString.asReadOnlyByteBuffer();
    logger.info("byte buffer: {}", byteBuffer);
    assertTrue(byteBuffer.remaining() == 90);
  }

  @Test
  public void buildAndParseWriteRequestNormal() {
    long requestId = 10000;
    long volumeId = 12345678;
    int segmentIndex = 100;
    PbWriteRequest.Builder writeRequestBuilder = PbWriteRequest.newBuilder();
    writeRequestBuilder.setRequestId(requestId);
    writeRequestBuilder.setVolumeId(volumeId);
    writeRequestBuilder
        .setMembership(
            PbRequestResponseHelper.buildPbMembershipFrom(TestUtils.generateMembership()));
    writeRequestBuilder.setSegIndex(segmentIndex);
    writeRequestBuilder.setFailTimes(1);
    List<PbWriteRequestUnit> unitList = new ArrayList<PbWriteRequestUnit>();
    writeRequestBuilder.addAllRequestUnits(unitList);
    writeRequestBuilder.setZombieWrite(false);
    writeRequestBuilder.setRequestTime(System.currentTimeMillis());
    PbWriteRequest writeRequest = null;
    writeRequest = writeRequestBuilder.build();

    PbWriteRequest newWriteRequest = null;
    try {
      newWriteRequest = PbWriteRequest.parseFrom(writeRequest.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      fail();
    }

    assertTrue(newWriteRequest.getSegIndex() == segmentIndex);
    assertTrue(newWriteRequest.getRequestUnitsList() != null);
    assertTrue(newWriteRequest.getRequestUnitsList().size() == 0);
  }

  @Test
  public void buildAndParseWriteRequestNormalFailed() {
    long requestId = 10000;
    long volumeId = 12345678;
    PbWriteRequest.Builder writeRequestBuilder = PbWriteRequest.newBuilder();
    writeRequestBuilder.setRequestId(requestId);
    writeRequestBuilder.setVolumeId(volumeId);
    writeRequestBuilder
        .setMembership(
            PbRequestResponseHelper.buildPbMembershipFrom(TestUtils.generateMembership()));
    // do not set required field
    writeRequestBuilder.setSegIndex(0);
    writeRequestBuilder.setFailTimes(0);
    writeRequestBuilder.setRequestTime(System.currentTimeMillis());
    final List<PbWriteRequestUnit> unitList = new ArrayList<PbWriteRequestUnit>();
    PbWriteRequestUnit.Builder builder = PbWriteRequestUnit.newBuilder();
    byte[] data = new byte[4096];
    for (int i = 0; i < data.length; i++) {
      data[i] = 1;
    }

    builder.setChecksum(0L);
    builder.setLength(4096);
    builder.setLogUuid(0L);
    builder.setLogId(0L);
    builder.setOffset(0L);
    unitList.add(builder.build());
    writeRequestBuilder.addAllRequestUnits(unitList);
    writeRequestBuilder.setZombieWrite(false);
    PbWriteRequest writeRequest = writeRequestBuilder.build();
    logger.info("write request: {}", writeRequest);
    byte[] requestArray = writeRequest.toByteArray();
    try {
      writeRequest = PbWriteRequest.parseFrom(requestArray);
    } catch (InvalidProtocolBufferException e) {
      logger.error("can not parse write request");
      fail();
    }
    for (int i = 0; i < requestArray.length; i++) {
      requestArray[i] = 0;
    }
    assertEquals(writeRequest.getRequestUnitsCount(), 1);

  }

  @Test
  public void buildAndParseWriteResponse() {
    long requestId = 10000;
    long logId = 1234;

    PbWriteResponse.Builder writeResponseBuilder = PbWriteResponse.newBuilder();
    writeResponseBuilder.setRequestId(requestId);
    final List<PbBroadcastLog> logList = new ArrayList<PbBroadcastLog>();
    PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
    Long uuid = RequestIdBuilder.get();
    logBuilder.setLogUuid(uuid);
    logBuilder.setLogId(logId);
    logBuilder.setOffset(90L);
    logBuilder.setLength(1290);
    logBuilder.setChecksum(0L);
    logBuilder.setLogStatus(PbBroadcastLogStatus.CREATED);
    logList.add(logBuilder.build());

    List<PbBroadcastLogManager> broadcastLogManagers = new ArrayList<PbBroadcastLogManager>();
    PbBroadcastLogManager.Builder broadcastLogManagerBuilder = PbBroadcastLogManager.newBuilder();
    broadcastLogManagerBuilder.setRequestId(requestId);
    broadcastLogManagerBuilder.addAllBroadcastLogs(logList);
    broadcastLogManagers.add(broadcastLogManagerBuilder.build());

    writeResponseBuilder.addAllLogManagersToCommit(broadcastLogManagers);

    final List<PbWriteResponseUnit> responseUnitList = new ArrayList<PbWriteResponseUnit>();

    PbWriteResponseUnit.Builder responseUnitBuilder = PbWriteResponseUnit.newBuilder();
    responseUnitBuilder.setLogUuid(uuid);
    responseUnitBuilder.setLogId(logId);
    responseUnitBuilder.setLogResult(PbIoUnitResult.OK);

    responseUnitList.add(responseUnitBuilder.build());
    writeResponseBuilder.addAllResponseUnits(responseUnitList);
    PbWriteResponse writeResponse = null;
    writeResponse = writeResponseBuilder.build();

    PbWriteResponse newWriteResponse = null;
    try {
      newWriteResponse = PbWriteResponse.parseFrom(writeResponse.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      fail();
    }
    assertTrue(newWriteResponse.getLogManagersToCommitList() != null);
    assertEquals(newWriteResponse.getLogManagersToCommitCount(), 1);
    PbBroadcastLogManager newLogManager = newWriteResponse.getLogManagersToCommit(0);
    assertTrue(newLogManager.getRequestId() == requestId);

    // check optional value, if don't set it, should be not exist
    assertTrue(newWriteResponse.hasMembership() == false);

    assertTrue(newWriteResponse.getResponseUnitsList() != null);
    assertEquals(newWriteResponse.getResponseUnitsCount(), 1);
    PbWriteResponseUnit newResponseUnit = newWriteResponse.getResponseUnits(0);
    assertTrue(newResponseUnit.getLogId() == logId);
  }

  @Test
  public void buildAndParseWriteResponseWithOptionalValue() {
    final long requestId = 10000;
    final long logId = 1234;
    long instanceId = 10000;
    Collection<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(instanceId++));
    secondaries.add(new InstanceId(instanceId++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(1, 0),
        new InstanceId(instanceId++),
        secondaries);
    PbMembership pbMembership = PbRequestResponseHelper.buildPbMembershipFrom(membership);

    PbWriteResponse.Builder writeResponseBuilder = PbWriteResponse.newBuilder();
    writeResponseBuilder.setMembership(pbMembership);
    writeResponseBuilder.setRequestId(requestId);
    final List<PbBroadcastLog> logList = new ArrayList<PbBroadcastLog>();
    PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
    Long uuid = RequestIdBuilder.get();
    logBuilder.setLogUuid(uuid);
    logBuilder.setLogId(logId);
    logBuilder.setOffset(90L);
    logBuilder.setLength(1290);
    logBuilder.setChecksum(0L);
    logBuilder.setLogStatus(PbBroadcastLogStatus.CREATED);
    logList.add(logBuilder.build());

    List<PbBroadcastLogManager> broadcastLogManagers = new ArrayList<PbBroadcastLogManager>();
    PbBroadcastLogManager.Builder broadcastLogManagerBuilder = PbBroadcastLogManager.newBuilder();
    broadcastLogManagerBuilder.setRequestId(requestId);
    broadcastLogManagerBuilder.addAllBroadcastLogs(logList);
    broadcastLogManagers.add(broadcastLogManagerBuilder.build());

    writeResponseBuilder.addAllLogManagersToCommit(broadcastLogManagers);

    final List<PbWriteResponseUnit> responseUnitList = new ArrayList<PbWriteResponseUnit>();

    PbWriteResponseUnit.Builder responseUnitBuilder = PbWriteResponseUnit.newBuilder();
    responseUnitBuilder.setLogUuid(uuid);
    responseUnitBuilder.setLogId(logId);
    responseUnitBuilder.setLogResult(PbIoUnitResult.OK);
    responseUnitList.add(responseUnitBuilder.build());
    writeResponseBuilder.addAllResponseUnits(responseUnitList);
    PbWriteResponse writeResponse = null;
    writeResponse = writeResponseBuilder.build();

    PbWriteResponse newWriteResponse = null;
    try {
      newWriteResponse = PbWriteResponse.parseFrom(writeResponse.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      fail();
    }
    assertTrue(newWriteResponse.getLogManagersToCommitList() != null);
    assertEquals(newWriteResponse.getLogManagersToCommitCount(), 1);
    PbBroadcastLogManager newLogManager = newWriteResponse.getLogManagersToCommit(0);
    assertTrue(newLogManager.getRequestId() == requestId);

    // check optional value, if set it, should be exist
    assertTrue(newWriteResponse.hasMembership() == true);

    assertTrue(newWriteResponse.getResponseUnitsList() != null);
    assertEquals(newWriteResponse.getResponseUnitsCount(), 1);
    PbWriteResponseUnit newResponseUnit = newWriteResponse.getResponseUnits(0);
    assertTrue(newResponseUnit.getLogId() == logId);
  }

  @Test
  public void readMetadataAndWriteMetadataSize() throws Exception {
    final long requestId = 10000;
    long logId = 1234;
    long loguuid = 2234;
    long instanceId = 10000;
    Collection<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(instanceId++));
    secondaries.add(new InstanceId(instanceId++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(1, 0),
        new InstanceId(instanceId++),
        secondaries);
    PbMembership pbMembership = PbRequestResponseHelper.buildPbMembershipFrom(membership);

    PbWriteResponse.Builder writeResponseBuilder = PbWriteResponse.newBuilder();
    writeResponseBuilder.setMembership(pbMembership);
    writeResponseBuilder.setRequestId(requestId);
    List<PbBroadcastLog> logList = new ArrayList<PbBroadcastLog>();
    int logCount = 128;
    for (int i = 0; i < logCount; i++) {
      PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
      logBuilder.setLogUuid(loguuid);
      logBuilder.setLogId(logId);
      logBuilder.setOffset(90L);
      logBuilder.setLength(1290);
      logBuilder.setChecksum(0L);
      logBuilder.setLogStatus(PbBroadcastLogStatus.CREATED);
      logList.add(logBuilder.build());
    }

    List<PbBroadcastLogManager> broadcastLogManagers = new ArrayList<PbBroadcastLogManager>();
    PbBroadcastLogManager.Builder broadcastLogManagerBuilder = PbBroadcastLogManager.newBuilder();
    broadcastLogManagerBuilder.setRequestId(requestId);
    broadcastLogManagerBuilder.addAllBroadcastLogs(logList);
    broadcastLogManagers.add(broadcastLogManagerBuilder.build());

    writeResponseBuilder.addAllLogManagersToCommit(broadcastLogManagers);

    List<PbWriteResponseUnit> responseUnitList = new ArrayList<PbWriteResponseUnit>();
    int pageCount = 128;
    for (int i = 0; i < pageCount; i++) {
      PbWriteResponseUnit.Builder responseUnitBuilder = PbWriteResponseUnit.newBuilder();
      responseUnitBuilder.setLogUuid(loguuid);
      responseUnitBuilder.setLogId(logId);
      responseUnitBuilder.setLogResult(PbIoUnitResult.OK);
      responseUnitList.add(responseUnitBuilder.build());
    }
    writeResponseBuilder.addAllResponseUnits(responseUnitList);
    PbWriteResponse writeResponse = null;
    writeResponse = writeResponseBuilder.build();
    PbWriteResponse newWriteResponse = null;

    try {
      newWriteResponse = PbWriteResponse.parseFrom(writeResponse.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      fail();
    }

    logger.warn("serialize size: {}, {}", writeResponse.getSerializedSize(),
        newWriteResponse.getSerializedSize());
    assertEquals(logCount, newWriteResponse.getLogManagersToCommit(0).getBroadcastLogsCount());
    assertEquals(pageCount, newWriteResponse.getResponseUnitsCount());
  }
}
