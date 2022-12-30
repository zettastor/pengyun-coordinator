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
