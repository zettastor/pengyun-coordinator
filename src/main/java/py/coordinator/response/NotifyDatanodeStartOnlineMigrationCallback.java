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

import py.coordinator.lib.VolumeInfoHolder;
import py.instance.InstanceId;
import py.netty.core.AbstractMethodCallback;
import py.proto.Commitlog;


public class NotifyDatanodeStartOnlineMigrationCallback
    extends AbstractMethodCallback<Commitlog.PbStartOnlineMigrationResponse> {

  private VolumeInfoHolder volumeInfoHolder;
  private InstanceId notifyDatanode;

  public NotifyDatanodeStartOnlineMigrationCallback(VolumeInfoHolder volumeInfoHolder,
      InstanceId notifyDatanode) {
    this.volumeInfoHolder = volumeInfoHolder;
    this.notifyDatanode = notifyDatanode;
  }

  @Override
  public void complete(Commitlog.PbStartOnlineMigrationResponse object) {
    this.volumeInfoHolder.decNotifyDatanodeCount();
  }

  @Override
  public void fail(Exception e) {
    this.volumeInfoHolder.addFailedDatanode(this.notifyDatanode);
    this.volumeInfoHolder.decNotifyDatanodeCount();
  }

}
