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

package py.coordinator.iorequest.iorequest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.List;
import java.util.concurrent.Semaphore;
import py.coordinator.iorequest.iounit.IoUnit;


public interface IoRequest {

  public long getOffset();

  public long getLength();

  public ByteBuf getBody();

  public int getIoSum();

  public void releaseBody();

  public IoRequestType getIoRequestType();

  /**
   * reply to io request initiator.
   */
  public void reply();

  public int decReferenceCount();

  public int incReferenceCount();

  public int getReferenceCount();

  public void add(IoUnit ioUnit);

  public List<IoUnit> getIoUnits();

  public void getTicket(Semaphore ioDepth);

  public void setTicketForRelease(Semaphore ioDepth);

  public Channel getChannel();

  public long getVolumeId();

  public void setVolumeId(Long volumeId);

}
