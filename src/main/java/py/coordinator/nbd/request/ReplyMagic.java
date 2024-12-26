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
