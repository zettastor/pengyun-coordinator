/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
