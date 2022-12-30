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

package py.coordinator.pbrequest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.log.BroadcastLog;
import py.coordinator.logmanager.WriteIoContextManager;
import py.coordinator.utils.PbRequestResponsePbHelper;
import py.membership.SegmentMembership;
import py.netty.datanode.PyWriteRequest;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;


public class WriteRequestBuilder implements RequestBuilder<PyWriteRequest> {

  protected static final Logger logger = LoggerFactory.getLogger(WriteRequestBuilder.class);
  private final IoRequestType requestType;
  private PbWriteRequest.Builder builder;
  private ByteBuf unitsData;



  public WriteRequestBuilder(IoRequestType ioRequestType) {
    Validate.isTrue(ioRequestType.isWrite() || ioRequestType.isDiscard());
    this.requestType = ioRequestType;
    this.builder = PbWriteRequest.newBuilder();
    this.builder.setFailTimes(0);
  }

  public WriteRequestBuilder setRequestId(long requestId) {
    builder.setRequestId(requestId);
    return this;
  }



  public WriteRequestBuilder setSegId(SegId segId) {
    builder.setVolumeId(segId.getVolumeId().getId());
    builder.setSegIndex(segId.getIndex());
    return this;
  }


  public WriteRequestBuilder setFailTimes(int failTimes) {
    builder.setFailTimes(failTimes);
    return this;
  }

  public long getRequestTime() {
    return builder.getRequestTime();

  }

  public WriteRequestBuilder setRequestTime(long requestTime) {
    builder.setRequestTime(requestTime);
    return this;
  }



  public void setLogsToCreate(Collection<BroadcastLog> logs) {
    List<PbWriteRequestUnit> requestUnits = new ArrayList<>();

    int numLogs = logs.size();
    Validate.isTrue(numLogs >= 1);
    unitsData = null;
    if (numLogs > 1) {
      unitsData = new CompositeByteBuf(PooledByteBufAllocatorWrapper.INSTANCE, true, numLogs);
    }

    for (BroadcastLog broadcastLog : logs) {
      Validate.isTrue(!broadcastLog.isCreateSuccess());

      requestUnits.add(PbRequestResponsePbHelper.buildPbWriteRequestUnitFrom(broadcastLog));

      if (this.requestType.isWrite()) {
        ByteBuf buffer = broadcastLog.getPyBuffer().getByteBuf();
        Validate.isTrue(buffer.readableBytes() == broadcastLog.getLength());

        if (numLogs > 1) {
          ((CompositeByteBuf) unitsData).addComponent(true, buffer);
        } else {
          unitsData = buffer;
        }
      }
    }

    builder.clearRequestUnits();
    builder.addAllRequestUnits(requestUnits);
  }

  public void setSegmentMembership(SegmentMembership segmentMembership) {
    builder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(segmentMembership));
  }



  public void setManagersToCommit(List<WriteIoContextManager> managers) {
    if (managers.size() == 0) {
      return;
    }

    builder.clearBroadcastManagers();
    List<PbBroadcastLogManager> broadcastManagers = new ArrayList<>();
    List<PbBroadcastLog> broadcastLogs = new ArrayList<>();

    for (WriteIoContextManager manager : managers) {
      Validate.isTrue(!manager.isAllFinalStatus());
      PbBroadcastLogManager.Builder managerBuilder = PbBroadcastLogManager.newBuilder();
      managerBuilder.setRequestId(manager.getRequestId());
      for (BroadcastLog broadcastLog : manager.getLogsToCommit()) {

        broadcastLogs.add(PbRequestResponsePbHelper.buildPbBroadcastLogFrom(broadcastLog));
      }

      managerBuilder.addAllBroadcastLogs(broadcastLogs);
      broadcastManagers.add(managerBuilder.build());
      broadcastLogs.clear();
    }

    builder.addAllBroadcastManagers(broadcastManagers);
  }

  @Override
  public PyWriteRequest getRequest() {
    return new PyWriteRequest(builder.build(), unitsData, true);
  }

  @Override
  public IoRequestType getRequestType() {
    return this.requestType;
  }

  public PbWriteRequest getPbWriteRequest() {
    return builder.build();
  }

  public void setZombieWrite(boolean zombieWrite) {
    builder.setZombieWrite(zombieWrite);
  }

  public void setUnstablePrimaryWrite(boolean unstablePrimaryWrite) {
    builder.setUnstablePrimaryWrite(unstablePrimaryWrite);
  }
}
