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

import py.buffer.PyBuffer;


public class ReadIoUnitImpl extends BaseIoUnit {

  private PyBuffer pyBuffer;

  public ReadIoUnitImpl(int segIndex, long pageIndex, long offset, int length) {
    super(segIndex, pageIndex, offset, length);
  }

  @Override
  public String toString() {
    return "ReadIOUnitImpl [super=" + super.toString() + "]";
  }

  @Override
  public void releaseReference() {
    if (pyBuffer != null) {
      pyBuffer.releaseReference();
      pyBuffer = null;
    }
  }

  public PyBuffer getPyBuffer() {
    return pyBuffer;
  }

  public void setPyBuffer(PyBuffer pyBuffer) {
    this.pyBuffer = pyBuffer;
  }
}
