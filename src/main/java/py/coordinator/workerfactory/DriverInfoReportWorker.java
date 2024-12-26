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

package py.coordinator.workerfactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.VolumeInfoHolderManager;
import py.coordinator.nbd.PydClientManager;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverTypeThrift;


public class DriverInfoReportWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DriverInfoReportWorker.class);

  private InstanceStore instanceStore;
  private InstanceId driverContainerId;
  private NbdConfiguration nbdConfiguration;
  private PydClientManager pydClientManager;
  private DriverContainerClientFactory driverContainerClientFactory;
  private String checkString = "/";

  private VolumeInfoHolderManager volumeInfoHolderManager;



  public DriverInfoReportWorker(InstanceStore instanceStore, InstanceId driverContainerId,
      NbdConfiguration nbdConfiguration, PydClientManager pydClientManager,
      VolumeInfoHolderManager volumeInfoHolderManager) {
    Validate.notNull(instanceStore);
    Validate.notNull(driverContainerId);
    this.instanceStore = instanceStore;
    this.driverContainerId = driverContainerId;
    this.nbdConfiguration = nbdConfiguration;
    this.pydClientManager = pydClientManager;
    this.volumeInfoHolderManager = volumeInfoHolderManager;
    driverContainerClientFactory = new DriverContainerClientFactory(1);
    driverContainerClientFactory.setInstanceStore(this.instanceStore);
  }

  @Override
  public void doWork() throws Exception {
    try {
      Instance driverContainer = instanceStore.get(driverContainerId);
      if (driverContainer == null) {
        logger.warn("can not get driver container:{} to report driver", driverContainerId);
      } else {
        ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
        request.setRequestId(RequestIdBuilder.get());

        DriverMetadataThrift driverMetadataThrift = buildReportInfo();
        request.addToDrivers(driverMetadataThrift);
        EndPoint driverContainerEndPoint = driverContainer.getEndPoint();

        logger.debug("report request:{} to driver container:{} at:{}", request, driverContainerId,
            driverContainerEndPoint);
        DriverContainer.Iface driverContainerClient = driverContainerClientFactory
            .build(driverContainerEndPoint).getClient();
        driverContainerClient.reportDriverMetadata(request);
      }
    } catch (Exception e) {
      logger.warn("caught an exception when report driver meta data", e);
    }
  }

  protected DriverMetadataThrift buildReportInfo() {

    final long volumeId = nbdConfiguration.getVolumeId();
    logger.info("DriverInfoReportWorker  :{} ,SnapshotId:{},", nbdConfiguration.getVolumeId(),
        nbdConfiguration.getSnapshotId());
    DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setDriverContainerId(nbdConfiguration.getDriverContainerId());
    driverMetadataThrift
        .setDriverType(DriverTypeThrift.valueOf(nbdConfiguration.getDriverType().name()));
    driverMetadataThrift.setSnapshotId(0);
    driverMetadataThrift
        .setVolumeId(volumeInfoHolderManager.getVolumeInfoHolder(volumeId).getVolumeId());
    driverMetadataThrift.setInstanceId(nbdConfiguration.getDriverId());
    Map<String, AccessPermissionTypeThrift> clientInfo = new HashMap<>();
    for (Map.Entry<SocketAddress, PydClientManager.ClientInfo> entry : pydClientManager
        .getClientInfoMap()
        .entrySet()) {
      String connectHostname = entry.getKey().toString();
      if (!connectHostname.startsWith(checkString)) {
        int index = connectHostname.indexOf(checkString);
        if (index != -1) {
          connectHostname = connectHostname.substring(index);
        }
      }
      AccessPermissionTypeThrift accessPermissionTypeThrift = AccessPermissionTypeThrift
          .valueOf(entry.getValue().getAccessPermissionType().name());
      clientInfo.put(connectHostname, accessPermissionTypeThrift);
    }

    driverMetadataThrift.setClientHostAccessRule(clientInfo);

    return driverMetadataThrift;
  }
}
