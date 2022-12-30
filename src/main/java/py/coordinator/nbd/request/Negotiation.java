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

import static py.coordinator.nbd.ProtocoalConstants.CLI_SERV_MAGIC;
import static py.coordinator.nbd.ProtocoalConstants.INIT_PASSWD;
import static py.coordinator.nbd.ProtocoalConstants.NO_FLAG;

import io.netty.buffer.ByteBuf;
import py.common.struct.EndPoint;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.nbd.ProtocoalConstants;


public class Negotiation {

  private final EndPoint endPoint;
  private long devSize = 0;

  public Negotiation(long devSize) {
    this.devSize = devSize;
    this.endPoint = null;
  }


  
  public Negotiation(ByteBuf byteBuf, EndPoint endPoint) {
    this.devSize = byteBuf.getLong(16);
    this.endPoint = endPoint;
    byteBuf.skipBytes(getNegotiateLength());
  }


  
  public Negotiation(ByteBuf byteBuf) {
    this.devSize = byteBuf.getLong(16);
    this.endPoint = null;
    byteBuf.skipBytes(getNegotiateLength());
  }

 
  public static int getNegotiateLength() {
    return INIT_PASSWD.length + CLI_SERV_MAGIC.length + 8 + NO_FLAG.length + 1 + 123;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }


  
  public void writeTo(ByteBuf byteBuf) {
    byteBuf.writeBytes(INIT_PASSWD);
    byteBuf.writeBytes(ProtocoalConstants.CLI_SERV_MAGIC);
    byteBuf.writeLong(devSize);
    byteBuf.writeBytes(ProtocoalConstants.NO_FLAG);
    byteBuf.writeBytes(ProtocoalConstants.PYD_VERSION_INFO);
    byteBuf.writeInt(CoordinatorConfigSingleton.getInstance().getIoDepth());
    byteBuf.writeBytes(ProtocoalConstants.ZERO_BLOCK);
  }

  public long getDevSize() {
    return devSize;
  }

  public void setDevSize(long devSize) {
    this.devSize = devSize;
  }

  @Override
  public String toString() {
    return "Negotiation{" + "devSize=" + devSize + ", endPoint=" + endPoint + '}';
  }
}
