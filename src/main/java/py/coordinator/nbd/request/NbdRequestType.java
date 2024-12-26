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


public enum NbdRequestType {
  Read(0) {
    @Override
    public boolean isRead() {
      return true;
    }
  },

  Write(1) {
    @Override
    public boolean isWrite() {
      return true;
    }
  },

  Disc(2),

  Flush(3),

  Discard(4) {
    @Override
    public boolean isDiscard() {
      return true;
    }
  },

  Heartbeat(5),

  ActiveHearbeat(6);
  private int value;

  private NbdRequestType(int value) {
    this.value = value;
  }



  public static NbdRequestType findByValue(int value) {
    switch (value) {
      case 0:
        return Read;
      case 1:
        return Write;
      case 2:
        return Disc;
      case 3:
        return Flush;
      case 4:
        return Discard;
      case 5:
        return Heartbeat;
      case 6:
        return ActiveHearbeat;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }

  public boolean isRead() {
    return false;
  }

  public boolean isWrite() {
    return false;
  }

  public boolean isDiscard() {
    return false;
  }
}
