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

package py.coordinator.iorequest.iounitcontext;

import java.util.concurrent.atomic.AtomicBoolean;
import py.archive.segment.SegId;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.logmanager.ClonedSourceVolumeReadListener;


public class SkipForSourceVolumeIoUnitContext implements IoUnitContext {

  private IoRequestType ioRequestType;
  private IoUnit ioUnit;
  private SegId segId;
  private long pageIndex;
  private ClonedSourceVolumeReadListener listener;
  private AtomicBoolean hasDone = new AtomicBoolean(false);



  public SkipForSourceVolumeIoUnitContext(
      IoRequestType ioRequestType, IoUnit ioUnit, SegId segId, long pageIndex,
      ClonedSourceVolumeReadListener listener) {
    this.ioRequestType = ioRequestType;
    this.ioUnit = ioUnit;
    this.segId = segId;
    this.pageIndex = pageIndex;
    this.listener = listener;
  }

  public IoRequestType getRequestType() {
    return ioRequestType;
  }

  public IoUnit getIoUnit() {
    return ioUnit;
  }


  public void done() {
    if (listener != null) {
      listener.done();
    }
  }

  public int getSegIndex() {
    return segId.getIndex();
  }

  public long getPageIndexInSegment() {
    return pageIndex;
  }

  public IoRequest getIoRequest() {
    return null;
  }

  public void releaseReference(boolean isParticularlyWrite) {
    ioUnit = null;
  }


  @Override
  public boolean hasDone() {
    return hasDone.get();
  }


  @Override
  public int compareTo(IoUnitContext out) {
    if (out == null) {
      return 1;
    } else {
      long innOffset = ioUnit.getOffset();
      long outOffset = out.getIoUnit().getOffset();
      if (innOffset > outOffset) {
        return 1;
      } else if (innOffset == outOffset) {
        return 0;
      } else {
        return -1;
      }
    }
  }

  @Override
  public String toString() {
    return "SkipForSourceVolumeIOUnitContext{"
        + "ioRequestType=" + ioRequestType
        + ", ioUnit=" + ioUnit
        + ", segId=" + segId
        + ", pageIndex=" + pageIndex
        + ", listener=" + listener
        + '}';
  }
}
