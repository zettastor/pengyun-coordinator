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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.Util;
import py.exception.InvalidFormatException;



public abstract class RequestHeader {

  private static final Logger logger = LoggerFactory.getLogger(RequestHeader.class);
  protected final NbdRequestType requestType;
  protected final MagicType magicType;
  protected final long handler;

 
  protected final long offset;

 
  protected final long length;


  
  public RequestHeader(MagicType magicType, NbdRequestType requestType, long handler, long offset,
      int length) {
    this.magicType = magicType;
    this.requestType = requestType;
    this.offset = offset;
    this.length = length;
    this.handler = handler;
  }


  
  public RequestHeader(MagicType magicType, ByteBuf buffer) {
    this.magicType = magicType;

   
    int type = buffer.readInt();
    requestType = NbdRequestType.findByValue(type);
    if (requestType == null) {
      logger.error("request type:{} doesn't match, buffer: {}", type, Util.bytesToString(buffer));
      throw new InvalidFormatException("request type doesn't match. The type we have is " + type);
    }

   
    handler = buffer.readLong();

   
    offset = buffer.readLong();
    if ((requestType == NbdRequestType.Read || requestType == NbdRequestType.Write) && offset < 0) {
      logger.error("offset:{} is not correct.  bytes: {}", offset, Util.bytesToString(buffer));
     
      throw new InvalidFormatException("offset: " + offset + " is not correct.");
    }

   
    length = buffer.readUnsignedInt();
    if ((requestType.isRead() || requestType.isWrite() || requestType.isDiscard()) && length < 0) {
      logger.error("length:{} is not correct, buffer is {}", length, Util.bytesToString(buffer));
      throw new InvalidFormatException("length is not correct. the length we got is " + length);
    }

  }


  
  public void writeTo(ByteBuf buffer) {
    buffer.writeInt(magicType.getValue());
    buffer.writeInt(requestType.getValue());
    buffer.writeLong(handler);
    buffer.writeLong(offset);
   
    buffer.writeInt((int) length);
  }

  public long getLength() {
    return length;
  }

  public MagicType getMagicType() {
    return magicType;
  }

  public long getOffset() {
    return offset;
  }

  public long getHandler() {
    return handler;
  }

  public NbdRequestType getRequestType() {
    return requestType;
  }

  public abstract int getIoSum();

 
  public abstract long getNbdClientTimestamp();

  @Override
  public String toString() {
    return "RequestHeader{" + "requestType=" + requestType + ", magicType=" + magicType
        + ", handler=" + Long.toHexString(handler)
        + ", offset=" + offset + ", length=" + length + '}';
  }
}
