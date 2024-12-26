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
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.ProtocoalConstants;


public enum MagicType {
  NBD_NORMAL(ProtocoalConstants.NBD_STANDARD_MAGIC) {
    public ReplyMagic getReplyMagic() {
      return ReplyMagic.REPLY_NORMAL;
    }

    @Override
    public int getRequestLength() {
      return 28;
    }

    @Override
    public RequestHeader getRequestHeader(ByteBuf buffer) {
      return new NbdNormalRequestHeader(buffer);
    }
  },

  NBD_DEBUG(ProtocoalConstants.NBD_STANDARD_METRIC_MAGIC) {
    @Override
    public ReplyMagic getReplyMagic() {
      return ReplyMagic.REPLY_DEBUG;
    }

    @Override
    public int getRequestLength() {
      return 44;
    }

    @Override
    public boolean isDebug() {
      return true;
    }

    @Override
    public int getReplyLength() {
      return 4 + 4 + 8 + 16;
    }

    @Override
    public RequestHeader getRequestHeader(ByteBuf buffer) {
      return new NbdDebugRequestHeader(buffer);
    }
  },

  PYD_NORMAL(ProtocoalConstants.NBD_IOSUM_MAGIC) {
    @Override
    public ReplyMagic getReplyMagic() {
      return ReplyMagic.REPLY_NORMAL;
    }

    @Override
    public int getRequestLength() {
      return 32;
    }

    @Override
    public RequestHeader getRequestHeader(ByteBuf buffer) {
      return new PydNormalRequestHeader(buffer);
    }
  },

  PYD_DEBUG(ProtocoalConstants.NBD_IOSUM_METRIC_MAGIC) {
    @Override
    public ReplyMagic getReplyMagic() {
      return ReplyMagic.REPLY_DEBUG;
    }

    @Override
    public int getRequestLength() {
      return 48;
    }

    @Override
    public boolean isDebug() {
      return true;
    }

    @Override
    public int getReplyLength() {
      return 4 + 4 + 8 + 16;
    }

    @Override
    public RequestHeader getRequestHeader(ByteBuf buffer) {
      return new PydDebugRequestHeader(buffer);
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(MagicType.class);
  private final int value;

  MagicType(int value) {
    this.value = value;
  }

  public static int getMagicLength() {
    return 4;
  }


  
  public static int getMinResponseLength() {
    int size = 0;
    for (MagicType type : MagicType.values()) {
      if (size == 0) {
        size = type.getReplyLength();
        continue;
      }

      if (size > type.getReplyLength()) {
        size = type.getReplyLength();
      }
    }

    return size;
  }


  
  public static MagicType findByValue(int value) {
    switch (value) {
      case ProtocoalConstants.NBD_IOSUM_MAGIC:
        return MagicType.PYD_NORMAL;
      case ProtocoalConstants.NBD_IOSUM_METRIC_MAGIC:
        return MagicType.PYD_DEBUG;
      case ProtocoalConstants.NBD_STANDARD_MAGIC:
        return MagicType.NBD_NORMAL;
      case ProtocoalConstants.NBD_STANDARD_METRIC_MAGIC:
        return MagicType.NBD_DEBUG;
      default:
        logger.warn("the magic type is not found, magic={}", Integer.toOctalString(value));
        return null;
    }
  }

  public static Negotiation generateNegotiation(long devSize) {
    return new Negotiation(devSize);
  }

  public int getValue() {
    return value;
  }

  public boolean isDebug() {
    return false;
  }

  public ReplyMagic getReplyMagic() {
    throw new NotImplementedException();
  }

  public int getRequestLength() {
    throw new NotImplementedException();
  }

  public int getHeaderLength() {
    return getRequestLength() - getMagicLength();
  }

  public int getReplyLength() {
    return 4 + 4 + 8;
  }

  public RequestHeader getRequestHeader(ByteBuf buffer) {
    throw new org.apache.commons.lang3.NotImplementedException("");
  }
}
