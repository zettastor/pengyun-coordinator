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


public class Request {

  private final RequestHeader header;
  private ByteBuf body;

  public Request(RequestHeader header) {
    this(header, null);
  }

  public Request(RequestHeader header, ByteBuf body) {
    this.header = header;
    this.body = body;
  }

  public ByteBuf getBody() {
    return body;
  }

  public RequestHeader getHeader() {
    return header;
  }

 

  
  public boolean release() {
    boolean release = true;
    if (body != null) {
      release = body.release();
      body = null;
    }
    return release;
  }

  public void releaseReference() {
    body = null;
  }

  @Override
  public String toString() {
    return "Request [header=" + header + "]";
  }
}