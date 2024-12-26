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