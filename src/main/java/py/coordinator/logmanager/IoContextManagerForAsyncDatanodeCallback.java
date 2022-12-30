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

package py.coordinator.logmanager;

import py.instance.InstanceId;
import py.membership.IoActionContext;
import py.membership.IoMember;


public interface IoContextManagerForAsyncDatanodeCallback extends IoContextManagerCommon {

  public IoActionContext getIoActionContext();

  public void markDelay();

  public void markDoneDirectly();

  public void markRequestFailed(InstanceId whoIsDisconnect);

  public void markNeedUpdateMembership();

  public void noNeedUpdateMembership();

  public void resetNeedUpdateMembership();

  public boolean streamIO();

  public void replaceLogUuidForNotCreateCompletelyLogs();

  public void markPrimaryRsp();

  public void markSecondaryRsp();

  public void processResponse(Object object, IoMember ioMember);
}
