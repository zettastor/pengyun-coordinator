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

package py.coordinator.logmanager;

import py.archive.segment.SegId;


public class VolumeIdAndSegIdKey {

  private final Long volumeId;

  private final SegId segId;

  public VolumeIdAndSegIdKey(Long volumeId, SegId segId) {
    this.volumeId = volumeId;
    this.segId = segId;
  }

  public Long getVolumeId() {
    return volumeId;
  }

  public SegId getSegId() {
    return segId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VolumeIdAndSegIdKey)) {
      return false;
    }

    VolumeIdAndSegIdKey that = (VolumeIdAndSegIdKey) o;

    if (volumeId != null ? !volumeId.equals(that.volumeId) : that.volumeId != null) {
      return false;
    }
    return segId != null ? segId.equals(that.segId) : that.segId == null;
  }

  @Override
  public int hashCode() {
    int result = volumeId != null ? volumeId.hashCode() : 0;
    result = 31 * result + (segId != null ? segId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "VolumeIdAndSegIdKey{" + "volumeId=" + volumeId + ", segId=" + segId + '}';
  }
}
