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

package py.coordinator.task;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


public class BogusDelayed implements Delayed {

  private long delay;
  private long timeSettingDelay;

  public BogusDelayed() {
    delay = 0;
    timeSettingDelay = System.currentTimeMillis();
  }

  @Override
  public int compareTo(Delayed o) {
    if (o == null) {
      return 1;
    }

    if (o == this) {
      return 0;
    }

    long d = (getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
    return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(getExpireTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  private long getExpireTime() {
    return delay + timeSettingDelay;
  }
}
