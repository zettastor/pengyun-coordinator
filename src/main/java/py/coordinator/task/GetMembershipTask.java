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

import py.archive.segment.SegId;
import py.membership.SegmentMembership;


public class GetMembershipTask extends SingleTask {

  private final Long volumeId;
  private final SegId segId;
 
  private final Long requestId;

  private final SegmentMembership segmentMembership;


  
  public GetMembershipTask(Long volumeId, SegId segId, Long requestId,
      SegmentMembership segmentMembership) {
    this.volumeId = volumeId;
    this.requestId = requestId;
    this.segId = segId;
    this.segmentMembership = segmentMembership;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetMembershipTask)) {
      return false;
    }

    GetMembershipTask that = (GetMembershipTask) o;

    return segId != null ? segId.equals(that.segId) : that.segId == null;
  }

  @Override
  public int hashCode() {
    return segId != null ? segId.hashCode() : 0;
  }

  @Override
  public int compareTo(Object other) {
    int notEqual = -1;
    GetMembershipTask compareObject = (GetMembershipTask) other;
    if (equals(compareObject)) {
      return 0;
    }
    return notEqual;
  }

  @Override
  public String toString() {
    return "GetMembershipTask{" + "segId=" + segId + '}';
  }

  @Override
  public Object getCompareKey() {
    return segId;
  }

  @Override
  public Long getVolumeId() {
    return volumeId;
  }

  @Override
  public Long getRequestId() {
    return requestId;
  }

  public SegmentMembership getSegmentMembership() {
    return segmentMembership;
  }

}
