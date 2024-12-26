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

import py.buffer.PyBuffer;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.icshare.BroadcastLogStatus;
import py.io.sequential.IoSequentialTypeHolder;


public class BroadcastLog implements IoSequentialTypeHolder {


  private final int snapshotVersion;
  private final Runnable doneListener;


  private long logUuid;
  private long logId;
  private long pageIndexInSegment;
  private long offset;
  private int length;
  private IoSequentialType ioSequentialType;

  private BroadcastLogStatus status;

  private IoUnitContext ioContext;
  private long checksum;

  public BroadcastLog(long logUuid) {
    this(logUuid, null, 0);
  }

  public BroadcastLog(long logUuid, IoUnitContext ioContext, int snapshotVersion) {
    this(logUuid, ioContext, snapshotVersion, null);
  }



  public BroadcastLog(long logUuid, IoUnitContext ioContext, int snapshotVersion,
      Runnable doneListener) {
    this.logUuid = logUuid;
    this.status = BroadcastLogStatus.Creating;
    this.snapshotVersion = snapshotVersion;
    this.logId = 0;
    this.checksum = 0;
    this.doneListener = doneListener;
    this.ioSequentialType = IoSequentialType.UNKNOWN;
    if (ioContext != null) {
      this.ioContext = ioContext;
      this.pageIndexInSegment = ioContext.getPageIndexInSegment();
      this.offset = ioContext.getIoUnit().getOffset();
      this.length = ioContext.getIoUnit().getLength();
    }
  }



  public BroadcastLog clone() {
    BroadcastLog clone = new BroadcastLog(logUuid, ioContext, snapshotVersion);
    clone.setLogId(logId);
    return clone.setStatus(status).setChecksum(checksum);
  }

  public long getPageIndexInSegment() {
    return this.pageIndexInSegment;
  }

  public long getLogId() {
    return logId;
  }

  public void setLogId(long logId) {
    this.logId = logId;
  }

  public boolean isFinalStatus() {
    return status == BroadcastLogStatus.Committed || status == BroadcastLogStatus.AbortConfirmed;
  }

  public BroadcastLogStatus getStatus() {
    return status;
  }

  public BroadcastLog setStatus(BroadcastLogStatus status) {
    this.status = status;
    return this;
  }

  public boolean isCreateSuccess() {
    return status == BroadcastLogStatus.Created;
  }

  public boolean isCreateCompletion() {
    return status == BroadcastLogStatus.Created || status == BroadcastLogStatus.Abort;
  }

  public long getOffset() {
    return this.offset;
  }

  public int getLength() {
    return this.length;
  }

  public PyBuffer getPyBuffer() {
    return getIoContext().getIoUnit().getPyBuffer();
  }

  public long getChecksum() {
    return this.checksum;
  }

  public BroadcastLog setChecksum(long checksum) {
    this.checksum = checksum;
    return this;
  }

  public int getSnapshotVersion() {
    return snapshotVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BroadcastLog)) {
      return false;
    }

    BroadcastLog that = (BroadcastLog) o;

    return logUuid == that.logUuid;
  }

  @Override
  public int hashCode() {
    return (int) (logUuid ^ (logUuid >>> 32));
  }



  public void release() {
    IoUnit unit = getIoContext().getIoUnit();
    PyBuffer buffer = unit.getPyBuffer();
    if (buffer != null) {
      unit.setPyBuffer(null);
      buffer.release();
    }
  }



  public void done() {
    getIoContext().done();
    releaseReference();
    if (doneListener != null) {
      doneListener.run();
    }
  }

  private void releaseReference() {
    if (ioContext != null) {
      ioContext.releaseReference(true);
      ioContext = null;
    }
  }

  @Override
  public IoSequentialType getIoSequentialType() {
    return ioSequentialType;
  }

  @Override
  public void setIoSequentialType(IoSequentialType ioSequentialType) {
    this.ioSequentialType = ioSequentialType;
  }


  public boolean isRandom() {
    return ioSequentialType != IoSequentialType.SEQUENTIAL_TYPE;
  }

  public long getLogUuid() {
    return logUuid;
  }

  public void setLogUuid(long logUuid) {
    this.logUuid = logUuid;
  }

  @Override
  public String toString() {
    return "BroadcastLog{"
        + "logUUID=" + logUuid
        + ", logId=" + logId
        + ", pageIndexInSegment=" + pageIndexInSegment
        + ", offset=" + offset
        + ", length=" + length
        + ", status=" + status
        + ", snapshotVersion=" + snapshotVersion
        + '}';
  }

  public IoUnitContext getIoContext() {
    return ioContext;
  }

  protected Runnable getDoneListener() {
    return doneListener;
  }
}
