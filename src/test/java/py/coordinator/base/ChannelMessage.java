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

import java.util.List;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.nbd.request.NbdRequestType;

/**
 * xx.
 */
public class ChannelMessage {

  private final NbdRequestType requestType;
  private final List<IoRequest> ioRequests;
  private final int ioSize;


  /**
   * xx.
   */
  public ChannelMessage(List<IoRequest> ioRequests, NbdRequestType requestType, int ioSize) {
    this.ioRequests = ioRequests;
    this.requestType = requestType;
    this.ioSize = ioSize;
  }

  public NbdRequestType getRequestType() {
    return requestType;
  }

  public List<IoRequest> getIoRequests() {
    return ioRequests;
  }

  public int getIoSize() {
    return ioSize;
  }

}
