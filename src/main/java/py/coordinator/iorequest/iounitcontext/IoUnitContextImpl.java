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
