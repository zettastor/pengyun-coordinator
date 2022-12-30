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

package py.coordinator.workerfactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.configuration.NbdConfiguration;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.icshare.DriverKey;
import py.icshare.qos.IoLimitScheduler;
import py.infocenter.client.InformationCenterClientFactory;
import py.informationcenter.AccessPermissionType;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.periodic.Worker;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.GetIoLimitationByDriverRequestThrift;
import py.thrift.share.GetIoLimitationByDriverResponseThrift;
import py.thrift.share.GetIoLimitationRequestThrift;
import py.thrift.share.GetIoLimitationResponseThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsRequest;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.VolumeAccessRuleThrift;


public class PullVolumeAccessRulesWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(PullVolumeAccessRulesWorker.class);

  private final InstanceStore instanceStore;
  private NbdConfiguration nbdConfiguration;


  private Map<String, AccessPermissionType> volumeAccessRuleTable;

  private InformationCenterClientFactory informationCenterClientFactory;
  private DriverContainerClientFactory driverContainerClientFactory;
  private IoLimitScheduler ioLimitScheduler;



  public PullVolumeAccessRulesWorker(InstanceStore instanceStore, NbdConfiguration nbdConfiguration,
      IoLimitScheduler ioLimitScheduler) {
    Validate.notNull(nbdConfiguration);
    this.volumeAccessRuleTable = nbdConfiguration.getVolumeAccessRules();
    this.instanceStore = instanceStore;
    this.nbdConfiguration = nbdConfiguration;
    this.ioLimitScheduler = ioLimitScheduler;

    informationCenterClientFactory = new InformationCenterClientFactory(1);
    informationCenterClientFactory.setInstanceName(PyService.INFOCENTER.getServiceName());
    informationCenterClientFactory.setInstanceStore(instanceStore);
    driverContainerClientFactory = new DriverContainerClientFactory(1);
    driverContainerClientFactory.setInstanceStore(instanceStore);
  }

  @Override
  public void doWork() throws Exception {
    ListVolumeAccessRulesByVolumeIdsResponse response = null;
    DriverKey driverKey = new DriverKey(nbdConfiguration.getDriverContainerId(),
        nbdConfiguration.getVolumeId(),
        nbdConfiguration.getSnapshotId(), nbdConfiguration.getDriverType());
    try {

      logger.info("going to pull volume:{} access rules from driver container:{}, SnapshotId :{}",
          driverKey.getVolumeId(),
          driverKey.getDriverContainerId(), driverKey.getSnapshotId());

      ListVolumeAccessRulesByVolumeIdsRequest listVolumeAccessRulesByVolumeIdsRequest =
          new ListVolumeAccessRulesByVolumeIdsRequest();
      listVolumeAccessRulesByVolumeIdsRequest.setRequestId(RequestIdBuilder.get());
      listVolumeAccessRulesByVolumeIdsRequest.addToVolumeIds(driverKey.getVolumeId());

      Instance driverContainer = instanceStore
          .get(new InstanceId(driverKey.getDriverContainerId()));
      Validate.notNull(driverContainer);
      EndPoint driverContainerEndPoint = driverContainer.getEndPoint();
      int tryTimes = 3;
      while (tryTimes-- > 0) {
        try {
          DriverContainer.Iface driverContainerClient = driverContainerClientFactory
              .build(driverContainerEndPoint).getClient();
          response = driverContainerClient
              .listVolumeAccessRulesByVolumeIds(listVolumeAccessRulesByVolumeIdsRequest);
          logger.info("get volume access rules from driver container:{}, response:{}",
              driverKey.getDriverContainerId(),
              response);
          break;
        } catch (Exception e) {
          logger.warn("caught an exception get volume access rules from driver container", e);
        }
      }

      if (tryTimes <= 0) {
        try {

          InformationCenter.Iface infoCenterClient = informationCenterClientFactory.build()
              .getClient();
          logger.warn(
              "can not pull volume access rules from driver container:{}, try pull from infocenter",
              driverKey.getDriverContainerId());
          response = infoCenterClient
              .listVolumeAccessRulesByVolumeIds(listVolumeAccessRulesByVolumeIdsRequest);
          logger.warn("get volume access rules from infocenter:{}", response);
        } catch (Exception e) {
          logger.warn("caught an exception get volume access rules from info center", e);
        }
      }

      if (response != null && response.getAccessRulesTable() != null
          && response.getAccessRulesTableSize() > 0) {

        synchronized (volumeAccessRuleTable) {
          volumeAccessRuleTable.clear();
          for (Map.Entry<Long, List<VolumeAccessRuleThrift>> entry : response.getAccessRulesTable()
              .entrySet()) {
            Validate.isTrue(entry.getKey().longValue() == driverKey.getVolumeId());
            for (VolumeAccessRuleThrift ruleThrift : entry.getValue()) {
              AccessPermissionType access = AccessPermissionType
                  .findByValue(ruleThrift.getPermission().getValue());
              volumeAccessRuleTable.put(ruleThrift.getIncomingHostName(), access);
            }
          }
        }
      }
    } catch (Throwable t) {
      logger.error("caught an exception when try get volume access rule from driver container", t);
    }

    doPullIoLimitationWork(driverKey);
  }

  private void doPullIoLimitationWork(DriverKey driverKey) {

    GetIoLimitationByDriverResponseThrift response = null;
    try {
      DriverKeyThrift driverKeyThrift = new DriverKeyThrift();
      driverKeyThrift.setDriverContainerId(driverKey.getDriverContainerId());
      driverKeyThrift.setVolumeId(driverKey.getVolumeId());
      driverKeyThrift.setDriverType(DriverTypeThrift.valueOf(driverKey.getDriverType().name()));
      driverKeyThrift.setSnapshotId(driverKey.getSnapshotId());
      logger.info("going to pull:{} io limitations from driver container:{}", driverKeyThrift,
          driverKey.getDriverContainerId());

      GetIoLimitationByDriverRequestThrift getIoLimitationByDriverRequestThrift =
          new GetIoLimitationByDriverRequestThrift();
      getIoLimitationByDriverRequestThrift.setRequestId(RequestIdBuilder.get());

      getIoLimitationByDriverRequestThrift.setDriverKeyThrift(driverKeyThrift);

      Instance driverContainer = instanceStore
          .get(new InstanceId(driverKey.getDriverContainerId()));
      Validate.notNull(driverContainer);
      EndPoint driverContainerEndPoint = driverContainer.getEndPoint();

      int tryTimes = 3;
      while (tryTimes-- > 0) {
        try {
          DriverContainer.Iface driverContainerClient = driverContainerClientFactory
              .build(driverContainerEndPoint).getClient();
          response = driverContainerClient
              .getIoLimitationsByDriver(getIoLimitationByDriverRequestThrift);
          logger.info("get io limitations from driver container:{}, response:{}",
              driverKey.getDriverContainerId(),
              response);
          break;
        } catch (Exception e) {
          logger.warn("caught an exception get io limitations from driver container", e);
        }
      }

      GetIoLimitationResponseThrift getIoLimitationResponseThrift = null;
      if (tryTimes <= 0) {
        try {

          logger.warn(
              "can not pull io limitations from driver container:{}, try pull from infocenter",
              driverKey.getDriverContainerId());
          GetIoLimitationRequestThrift requestThrift = new GetIoLimitationRequestThrift();
          requestThrift.setRequestId(RequestIdBuilder.get());
          requestThrift.setDriverContainerId(driverKey.getDriverContainerId());
          InformationCenter.Iface infoCenterClient = informationCenterClientFactory.build()
              .getClient();
          getIoLimitationResponseThrift = infoCenterClient
              .getIoLimitationsInOneDriverContainer(requestThrift);
          logger.warn("get io limitations from infocenter:{}", getIoLimitationResponseThrift);
        } catch (Exception e) {
          logger.warn("caught an exception get io limitations from info center", e);
        }
      }

      if (response != null) {

        processPullIoLimitation(response.getIoLimitationsList());
      } else if (getIoLimitationResponseThrift != null) {

        Map<DriverKeyThrift, List<IoLimitationThrift>> driver2IoLimitationList =
            getIoLimitationResponseThrift
                .getMapDriver2ItsIoLimitations();
        if (driver2IoLimitationList != null && !driver2IoLimitationList.isEmpty()) {
          for (Map.Entry<DriverKeyThrift, List<IoLimitationThrift>> entry : driver2IoLimitationList
              .entrySet()) {
            DriverKeyThrift driverKeyThrift1 = entry.getKey();
            DriverKey getDriverKey = RequestResponseHelper.buildDriverKeyFrom(driverKeyThrift1);
            if (getDriverKey.equals(driverKey)) {
              processPullIoLimitation(entry.getValue());
            }
          }
        }
      } else {

        logger.error("can not get any response from driver container or info center");
      }

    } catch (Throwable t) {
      logger.error("caught an exception when try get io limitations from driver container", t);
    }

  }


  private void processPullIoLimitation(List<IoLimitationThrift> ioLimitationThrifts) {
    if (ioLimitationThrifts == null || ioLimitationThrifts.isEmpty()) {
      logger.debug("clear all entries.");
      ioLimitScheduler.clearLimitationEntries();
      return;
    }

    Set<Long> gotIoLimitationEntryIds = new HashSet<>();
    boolean existStaticLimitation = false;
    for (IoLimitationThrift ioLimitationThrift : ioLimitationThrifts) {

      IoLimitation ioLimitation = RequestResponseHelper.buildIoLimitationFrom(ioLimitationThrift);
      for (IoLimitationEntry entry : ioLimitation.getEntries()) {
        gotIoLimitationEntryIds.add(entry.getEntryId());
      }
      if (ioLimitation.getLimitType().equals(IoLimitation.LimitType.Static)) {
        existStaticLimitation = true;
      }

      ioLimitScheduler.addOrModifyIoLimitation(ioLimitation);
    }

    if (!gotIoLimitationEntryIds.isEmpty()) {
      for (Long currentIoLimitationId : ioLimitScheduler.getIoLimitationEntryRecord().keySet()) {
        if (!gotIoLimitationEntryIds.contains(currentIoLimitationId)) {
          ioLimitScheduler.deleteIoLimitationEntry(currentIoLimitationId);
        }
      }
    }

    if (!existStaticLimitation) {
      ioLimitScheduler.deleteStaticLimitationEntry();
    }
  }
}