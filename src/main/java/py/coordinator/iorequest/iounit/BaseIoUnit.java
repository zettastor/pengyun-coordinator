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

package py.coordinator.iorequest.iounit;

import org.apache.commons.lang.NotImplementedException;
import py.buffer.PyBuffer;


public abstract class BaseIoUnit implements IoUnit {

  private final int segIndex;
  private final long pageIndex;

  
  private final long offset;

  
  private final int length;

  private volatile boolean success;


  
  public BaseIoUnit(int segIndex, long pageIndex, long offset, int length) {
    this.segIndex = segIndex;
    this.pageIndex = pageIndex;
    this.offset = offset;
    this.length = length;
    this.success = false;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public PyBuffer getPyBuffer() {
    throw new NotImplementedException("");
  }

  @Override
  public void setPyBuffer(PyBuffer pyBuffer) {
    throw new NotImplementedException("");
  }

  @Override
  public int getSegIndex() {
    return segIndex;
  }

  public long getPageIndexInSegment() {
    return pageIndex;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  @Override
  public String toString() {
    return "BaseIOUnit [segIndex=" + segIndex + ", pageIndex=" + pageIndex + ", offset=" + offset
        + ", length="
        + length + ", success=" + success + "]";
  }

}
