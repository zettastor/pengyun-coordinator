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

import org.apache.commons.lang3.NotImplementedException;


public class BogusSingleTask extends SingleTask {

  @Override
  public Object getCompareKey() {
    throw new NotImplementedException("not implement here");
  }

  @Override
  public Long getVolumeId() {
    return null;
  }

  @Override
  public Long getRequestId() {
    throw new NotImplementedException("not implement here");
  }

  @Override
  public int compareTo(Object o) {
    return -1;
  }
}
