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

package py.coordinator.nbd.request;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.Util;
import py.exception.InvalidFormatException;


public class PydNormalRequestHeader extends RequestHeader {

  private final Logger logger = LoggerFactory.getLogger(PydNormalRequestHeader.class);
 
  protected int ioSum;

  public PydNormalRequestHeader(NbdRequestType requestType, long handler, long offset, int length,
      int ioSum) {
    super(MagicType.PYD_NORMAL, requestType, handler, offset, length);
    this.ioSum = ioSum;
  }


  
  public PydNormalRequestHeader(ByteBuf buffer) {
    super(MagicType.PYD_NORMAL, buffer);
    ioSum = buffer.readInt();
    if (ioSum < 1) {
      logger.error("ioSum:{} is not correction, buffer is {}", ioSum, Util.bytesToString(buffer));
      throw new InvalidFormatException("io sum is not correction.the io sum we got is " + ioSum);
    }
  }

  @Override
  public void writeTo(ByteBuf buffer) {
    super.writeTo(buffer);
    buffer.writeInt(ioSum);
  }

  @Override
  public int getIoSum() {
    return ioSum;
  }

  @Override
  public long getNbdClientTimestamp() {
    throw new NotImplementedException("");
  }

  @Override
  public String toString() {
    return "PydNormalRequestHeader{super=" + super.toString() + "ioSum=" + ioSum + '}';
  }
}
