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
import py.common.DelayRequest;


public class CommitLogContext extends DelayRequest {

  private final Long volumeId;
  private final SegId segId;


  public CommitLogContext(Long volumeId, SegId segId, long delay) {
    super(delay);
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
    if (!(o instanceof CommitLogContext)) {
      return false;
    }

    CommitLogContext that = (CommitLogContext) o;

    if (volumeId != null ? !volumeId.equals(that.volumeId) : that.volumeId != null) {
      return false;
    }
    if (segId != null ? !segId.equals(that.segId) : that.segId != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = volumeId != null ? volumeId.hashCode() : 0;
    result = 31 * result + (segId != null ? segId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "CommitLogContext{" + "volumeId=" + volumeId + ", segId=" + segId
        + '}';
  }
}
