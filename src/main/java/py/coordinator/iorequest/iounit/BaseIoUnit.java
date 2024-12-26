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
