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

package py.coordinator.base;

import java.net.SocketAddress;
import java.util.List;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.MessageEvent;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.nbd.request.NbdRequestType;

/**
 * xx.
 */
public class MessageEventTest implements MessageEvent {

  private final Object object;

  public MessageEventTest(Object object) {
    this.object = object;
  }

  public MessageEventTest(List<IoRequest> ioRequests, NbdRequestType requestType, int ioSize) {
    this.object = new ChannelMessage(ioRequests, requestType, ioSize);
  }


  /**
   * xx.
   */
  public MessageEventTest(List<IoRequest> ioRequests, NbdRequestType requestType) {
    int ioSize = 0;
    for (IoRequest ioRequest : ioRequests) {
      ioSize += ioRequest.getLength();
    }
    this.object = new ChannelMessage(ioRequests, requestType, ioSize);
  }

  @Override
  public Channel getChannel() {
    return null;
  }

  @Override
  public ChannelFuture getFuture() {
    return null;
  }

  @Override
  public Object getMessage() {
    return object;
  }

  @Override
  public SocketAddress getRemoteAddress() {
    return null;
  }
}
