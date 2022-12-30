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
