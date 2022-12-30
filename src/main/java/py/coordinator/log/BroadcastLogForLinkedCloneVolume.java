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
