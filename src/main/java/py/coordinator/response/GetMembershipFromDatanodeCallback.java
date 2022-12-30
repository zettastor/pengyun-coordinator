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

package py.coordinator.response;

import py.instance.InstanceId;


public class GetMembershipFromDatanodeCallback implements TriggerByCheckCallback {

  private Long rootVolumeId;

  public GetMembershipFromDatanodeCallback(Long rootVolumeId) {
    this.rootVolumeId = rootVolumeId;
  }

  @Override
  public long getOriRequestId() {
    return 0;
  }

  @Override
  public void triggeredByCheckCallback() {

  }

  @Override
  public void markNeedUpdateMembership() {

  }

  @Override
  public void noNeedUpdateMembership() {

  }

  @Override
  public void resetNeedUpdateMembership() {

  }

  @Override
  public void markDoneDirectly() {

  }

  @Override
  public void markRequestFailed(InstanceId whoIsDisconnect) {

  }

  @Override
  public void resetRequestFailedInfo() {

  }

  @Override
  public boolean streamIO() {
    return false;
  }

  @Override
  public void doneForCommitLog() {

  }

  @Override
  public void replaceLogUuidForNotCreateCompletelyLogs() {

  }

  @Override
  public Long getVolumeId() {
    return this.rootVolumeId;
  }
}
