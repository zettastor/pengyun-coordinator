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
