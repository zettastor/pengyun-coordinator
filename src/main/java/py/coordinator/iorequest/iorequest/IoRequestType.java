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

package py.coordinator.iorequest.iorequest;


public enum IoRequestType {
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

  Discard(2) {
    @Override
    public boolean isDiscard() {
      return true;
    }
  };

  private int value;

  IoRequestType(int value) {
    this.value = value;
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
