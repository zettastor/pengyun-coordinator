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
