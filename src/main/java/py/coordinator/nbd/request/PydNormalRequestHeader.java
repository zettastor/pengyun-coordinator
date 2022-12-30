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
