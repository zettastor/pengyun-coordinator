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
