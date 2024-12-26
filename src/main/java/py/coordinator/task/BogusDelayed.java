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
