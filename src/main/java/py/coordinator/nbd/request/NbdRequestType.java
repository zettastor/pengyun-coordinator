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
