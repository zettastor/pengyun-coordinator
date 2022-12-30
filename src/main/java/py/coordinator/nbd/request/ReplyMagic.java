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

import static py.coordinator.nbd.ProtocoalConstants.NBD_HEARTBEAT_MAGIC;
import static py.coordinator.nbd.ProtocoalConstants.NBD_METRIC_REPLY_MAGIC;
import static py.coordinator.nbd.ProtocoalConstants.NBD_REPLY_MAGIC;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum ReplyMagic {

  REPLY_NORMAL(NBD_REPLY_MAGIC) {
    @Override
    public int getReplyLength() {
      return 16;
    }
  },

  REPLY_DEBUG(NBD_METRIC_REPLY_MAGIC) {
    @Override
    public int getReplyLength() {
      return 24;
    }
  },

  REPLY_HEARTBEAT(NBD_HEARTBEAT_MAGIC) {
    @Override
    public int getReplyLength() {
      return 16;
    }
  };

  private static final Logger logger = LoggerFactory.getLogger(ReplyMagic.class);
  private int value;

  ReplyMagic(int value) {
    this.value = value;
  }



  public static ReplyMagic findByValue(int value) {
    switch (value) {
      case NBD_REPLY_MAGIC:
        return REPLY_NORMAL;
      case NBD_METRIC_REPLY_MAGIC:
        return REPLY_DEBUG;
      case NBD_HEARTBEAT_MAGIC:
        return REPLY_HEARTBEAT;
      default:
        logger.warn("not found the reply magic={}", value);
        return null;
    }
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

  public int getValue() {
    return value;
  }

  public int getReplyLength() {
    throw new NotImplementedException("");
  }
}
