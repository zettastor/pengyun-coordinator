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
