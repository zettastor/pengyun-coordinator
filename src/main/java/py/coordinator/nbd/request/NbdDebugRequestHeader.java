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
