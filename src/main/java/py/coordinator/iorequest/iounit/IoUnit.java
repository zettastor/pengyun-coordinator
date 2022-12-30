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


public interface IoUnit {

  public long getOffset();

  public int getLength();

  public PyBuffer getPyBuffer();

  public void setPyBuffer(PyBuffer pyBuffer);

  public int getSegIndex();

  public boolean isSuccess();

  public void setSuccess(boolean success);

  public long getPageIndexInSegment();

  public void releaseReference();
}
