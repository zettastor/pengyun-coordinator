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
