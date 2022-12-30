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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadRequestUnit;


public class ReadRequestBuilder implements RequestBuilder<PbReadRequest> {

  private PbReadRequest.Builder builder;

  public ReadRequestBuilder() {
    builder = PbReadRequest.newBuilder();
    builder.setFailTimes(0);
  }

  public ReadRequestBuilder setRequestId(long requestId) {
    builder.setRequestId(requestId);
    return this;
  }

  public ReadRequestBuilder setFailTimes(int failTimes) {
    builder.setFailTimes(failTimes);
    return this;
  }



  public ReadRequestBuilder setSegId(SegId segId) {
    builder.setVolumeId(segId.getVolumeId().getId());
    builder.setSegIndex(segId.getIndex());
    return this;
  }

  public ReadRequestBuilder setReadCause(Broadcastlog.ReadCause readCause) {
    builder.setReadCause(readCause);
    return this;
  }

  public ReadRequestBuilder setLogsToCommit(List<Long> logsToCommit) {
    builder.addAllLogsToCommit(logsToCommit);
    return this;
  }

  @Override
  public PbReadRequest getRequest() {
    return builder.build();
  }

  @Override
  public IoRequestType getRequestType() {
    return IoRequestType.Read;
  }



  public void setRequestUnits(Collection<IoUnitContext> ioContexts) {
    List<PbReadRequestUnit> requestUnitList = new ArrayList<PbReadRequestUnit>();
    for (IoUnitContext ioContext : ioContexts) {
      IoUnit ioUnit = ioContext.getIoUnit();
      requestUnitList
          .add(PbRequestResponseHelper
              .buildPbReadRequestUnitFrom(ioUnit.getOffset(), (int) ioUnit.getLength()));
    }
    builder.clearRequestUnits();
    builder.addAllRequestUnits(requestUnitList);
  }



  public void setPbMembership(SegmentMembership membership) {
    Broadcastlog.PbMembership pbMembership = PbRequestResponseHelper
        .buildPbMembershipFrom(membership);
    builder.setMembership(pbMembership);
  }
}
