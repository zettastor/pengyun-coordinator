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

package py.coordinator.utils;

import py.RequestResponseHelper;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounit.WriteIoUnitImpl;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.iorequest.iounitcontext.IoUnitContextImpl;
import py.coordinator.log.BroadcastLog;
import py.coordinator.log.BroadcastLogForLinkedCloneVolume;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbReadRequestUnit;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.thrift.share.BroadcastLogThrift;


public class PbRequestResponsePbHelper {

 

  
  public static PbWriteRequestUnit buildPbWriteRequestUnitFrom(BroadcastLog broadcastLog) {
    PbWriteRequestUnit.Builder unitBuilder = PbWriteRequestUnit.newBuilder();
    unitBuilder.setLogUuid(broadcastLog.getLogUuid());
    unitBuilder.setLogId(broadcastLog.getLogId());
    unitBuilder.setOffset(broadcastLog.getOffset());
    unitBuilder.setLength(broadcastLog.getLength());
    unitBuilder.setChecksum(broadcastLog.getChecksum());
    if (broadcastLog instanceof BroadcastLogForLinkedCloneVolume) {
      BroadcastLogForLinkedCloneVolume logForLinkedCloneVolume =
          (BroadcastLogForLinkedCloneVolume) broadcastLog;
      unitBuilder.setSrcLength(logForLinkedCloneVolume.getSourceDataLength());
      unitBuilder.setSrcOffset(logForLinkedCloneVolume.getSourceDataOffset());
    }
    unitBuilder.setRandomWrite(broadcastLog.isRandom());
    return unitBuilder.build();
  }

 

  
  public static PbBroadcastLog buildPbBroadcastLogFrom(BroadcastLog broadcastLog) {
    PbBroadcastLog.Builder logBuilder = PbBroadcastLog.newBuilder();
    logBuilder.setLogUuid(broadcastLog.getLogUuid());
    logBuilder.setLogId(broadcastLog.getLogId());
    logBuilder.setOffset(broadcastLog.getOffset());
    logBuilder.setChecksum(broadcastLog.getChecksum());
    logBuilder.setLength(broadcastLog.getLength());
    logBuilder.setLogStatus(broadcastLog.getStatus().getPbLogStatus());
    return logBuilder.build();
  }


  
  public static PbReadRequestUnit buildPbReadRequestUnitFrom(IoUnit ioUnit) {
    PbReadRequestUnit.Builder builder = PbReadRequestUnit.newBuilder();
    builder.setOffset(ioUnit.getOffset());
    builder.setLength(ioUnit.getLength());
    return builder.build();
  }


  
  public static BroadcastLogThrift buildThriftBroadcastLogFrom(BroadcastLog log) {
    return new BroadcastLogThrift(log.getLogUuid(), log.getLogId(), log.getOffset(),
        log.getChecksum(),
        log.getLength(), log.getStatus().getThriftLogStatus(), log.getSnapshotVersion());
  }


  
  public static BroadcastLog buildBroadcastLogFrom(BroadcastLogThrift thriftLog, int index,
      long pageIndex) {
    IoUnitContext ioContext = new IoUnitContextImpl(null,
        new WriteIoUnitImpl(index, pageIndex, thriftLog.getOffset(), thriftLog.getLength(), null));
    BroadcastLog log = new BroadcastLog(thriftLog.getLogId(), ioContext,
        thriftLog.getSnapshotVersion());

    return log
        .setStatus(RequestResponseHelper.convertThriftStatusToStatus(thriftLog.getLogStatus()));
  }
}
