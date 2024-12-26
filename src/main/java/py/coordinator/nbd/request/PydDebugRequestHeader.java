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
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.Util;
import py.exception.InvalidFormatException;


public class PydDebugRequestHeader extends RequestHeader {

  private static final Logger logger = LoggerFactory.getLogger(PydDebugRequestHeader.class);
 
  protected int ioSum;

 
  private long nbdClientTimestamp;


  
  public PydDebugRequestHeader(ByteBuf buffer) {
    super(MagicType.PYD_DEBUG, buffer);
    ioSum = buffer.readInt();
    if (ioSum < 1) {
      logger.error("ioSum:{} is not correction,buffer is {}", ioSum, Util.bytesToString(buffer));
      throw new InvalidFormatException("io sum is not correction.the io sum we got is " + ioSum);
    }

   
    ByteBuf tmp = buffer.order(ByteOrder.LITTLE_ENDIAN);
    nbdClientTimestamp = tmp.readLong();
   
    tmp.readLong();
  }

  @Override
  public void writeTo(ByteBuf buffer) {
    super.writeTo(buffer);
    buffer.writeInt(ioSum);
    buffer.writeLong(nbdClientTimestamp);
    buffer.writeLong(0L);
  }

  @Override
  public int getIoSum() {
    return ioSum;
  }

  @Override
  public long getNbdClientTimestamp() {
    return nbdClientTimestamp;
  }

  @Override
  public String toString() {
    return "PydDebugRequestHeader{super=" + super.toString() + "ioSum=" + ioSum
        + ", nbdClientTimestamp="
        + nbdClientTimestamp + '}';
  }
}
