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

package py.coordinator.iofactory;

import java.util.Set;
import org.apache.commons.lang3.Validate;
import py.common.struct.EndPoint;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;


public abstract class AbstractIoFactory implements IoFactory {

  private InstanceStore instanceStore;
  private Set<InstanceId> subHealthyDatanodes;

  public AbstractIoFactory(InstanceStore instanceStore, Set<InstanceId> subHealthyDatanodes) {
    this.instanceStore = instanceStore;
    this.subHealthyDatanodes = subHealthyDatanodes;
  }



  public EndPoint getEndPoint(InstanceId instanceId) {
    Instance instance = instanceStore.get(instanceId);
    if (instance == null) {
      Validate.isTrue(false, "can not get instance by:" + instanceId);
    }
    return instance.getEndPointByServiceName(PortType.IO);
  }

  protected InstanceStore getInstanceStore() {
    return instanceStore;
  }

  protected Set<InstanceId> getSubHealthyDatanodes() {
    return subHealthyDatanodes;
  }
}
