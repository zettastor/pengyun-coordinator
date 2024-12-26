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
