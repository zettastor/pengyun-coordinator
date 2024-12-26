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

package py.coordinator.log;

import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.icshare.BroadcastLogStatus;


public class BroadcastLogForLinkedCloneVolume extends BroadcastLog {

  private final long sourceDataOffset;
  private final int sourceDataLength;
  private final BroadcastLog sourceLog;



  public BroadcastLogForLinkedCloneVolume(long logUuid, IoUnitContext ioContext,
      int snapshotVersion, long sourceDataOffset, int sourceDataLength, BroadcastLog log) {
    super(logUuid, ioContext, snapshotVersion, log.getDoneListener());

    this.sourceDataLength = sourceDataLength;
    this.sourceDataOffset = sourceDataOffset;
    this.sourceLog = log;
  }



  public BroadcastLogForLinkedCloneVolume(long logUuid) {
    super(logUuid);
    this.sourceDataLength = 0;
    this.sourceDataOffset = 0;
    this.sourceLog = null;
  }

  public void done() {
    sourceLog.getIoContext().getIoUnit().setSuccess(this.getIoContext().getIoUnit().isSuccess());
    super.done();
  }

  @Override
  public void setLogId(long logId) {
    sourceLog.setLogId(logId);
    super.setLogId(logId);
  }

  @Override
  public BroadcastLog setStatus(BroadcastLogStatus status) {
    sourceLog.setStatus(status);
    return super.setStatus(status);
  }

  public long getSourceDataOffset() {
    return sourceDataOffset;
  }

  public int getSourceDataLength() {
    return sourceDataLength;
  }

  @Override
  public String toString() {
    return "BroadcastLogForLinkedCloneVolume{"
        + "sourceDataOffset=" + sourceDataOffset
        + ", sourceDataLength=" + sourceDataLength
        + ", super =" + super.toString()
        + '}';
  }
}
