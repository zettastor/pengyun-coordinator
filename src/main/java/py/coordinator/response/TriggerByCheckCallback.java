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


public interface TriggerByCheckCallback {

  public long getOriRequestId();

  public void triggeredByCheckCallback();

  public void markNeedUpdateMembership();

  public void noNeedUpdateMembership();

  public void resetNeedUpdateMembership();

  public void markDoneDirectly();

  public void markRequestFailed(InstanceId whoIsDisconnect);

  public void resetRequestFailedInfo();

  public boolean streamIO();

  public void doneForCommitLog();

  public void replaceLogUuidForNotCreateCompletelyLogs();

  public Long getVolumeId();
}
