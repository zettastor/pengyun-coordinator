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


public class NbdDebugRequestHeader extends RequestHeader {


  private long nbdClientTimestamp;



  public NbdDebugRequestHeader(ByteBuf buffer) {
    super(MagicType.NBD_DEBUG, buffer);


    ByteBuf tmp = buffer.order(ByteOrder.LITTLE_ENDIAN);
    nbdClientTimestamp = tmp.readLong();
    tmp.readLong();
  }

  @Override
  public void writeTo(ByteBuf buffer) {
    super.writeTo(buffer);
    buffer.writeLong(nbdClientTimestamp);
    buffer.writeLong(0L);
  }

  @Override
  public int getIoSum() {
    return 1;
  }

  @Override
  public long getNbdClientTimestamp() {
    return nbdClientTimestamp;
  }

  @Override
  public String toString() {
    return "NbdDebugRequestHeader{ super=" + super.toString() + "nbdClientTimestamp="
        + nbdClientTimestamp + '}';
  }
}
