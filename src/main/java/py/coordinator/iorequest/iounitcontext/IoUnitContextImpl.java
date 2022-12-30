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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;


public class IoUnitContextImpl implements IoUnitContext {

  private static final Logger logger = LoggerFactory.getLogger(IoUnitContextImpl.class);
  protected IoUnit ioUnit;
  private IoRequest request;
  private AtomicBoolean hasDone = new AtomicBoolean(false);

  public IoUnitContextImpl(IoRequest request, IoUnit ioUnit) {
    this.ioUnit = ioUnit;
    this.request = request;
  }

  @Override
  public IoUnit getIoUnit() {
    return ioUnit;
  }

  @Override
  public long getPageIndexInSegment() {
    return ioUnit.getPageIndexInSegment();
  }

  @Override
  public int getSegIndex() {
    return ioUnit.getSegIndex();
  }

  @Override
  public IoRequestType getRequestType() {
    return request.getIoRequestType();
  }

  @Override
  public void done() {

    if (request == null) {
      return;
    }

    if (request.decReferenceCount() == 0) {
      request.reply();
      logger.debug("request: {} done", request);
    }
    request = null;

    hasDone.set(true);
  }

  @Override
  public boolean hasDone() {
    return hasDone.get();
  }

  @Override
  public IoRequest getIoRequest() {
    return request;
  }

  @Override
  public void releaseReference(boolean isParticularlyWrite) {
    if (ioUnit != null) {

      if (isParticularlyWrite) {
        ioUnit.releaseReference();
      }
      ioUnit = null;
    }
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
    return "IOUnitContextImpl{" + "request=" + request + ", ioUnit=" + ioUnit + '}';
  }
}
