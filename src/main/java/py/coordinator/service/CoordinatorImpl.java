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

package py.coordinator.service;

import static py.driver.DriverType.NBD;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.AbstractConfigurationServer;
import py.RequestResponseHelper;
import py.app.thrift.ThriftProcessorFactory;
import py.coordinator.CoordinatorAppEngine;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.nbd.NbdServer;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.worker.ExtendingVolumeUpdater;
import py.coordinator.workerfactory.DriverInfoReportWorkerFactory;
import py.coordinator.workerfactory.GracefulShutdownCoordinatorWorkerFactory;
import py.coordinator.workerfactory.PullVolumeAccessRulesWorkerFactory;
import py.exception.StorageException;
import py.icshare.qos.IoLimitScheduler;
import py.informationcenter.AccessPermissionType;
import py.periodic.PeriodicWorkExecutor;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.thrift.coordinator.service.AddOrModifyLimitationRequest;
import py.thrift.coordinator.service.AddOrModifyLimitationResponse;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.DeleteLimitationRequest;
import py.thrift.coordinator.service.DeleteLimitationResponse;
import py.thrift.coordinator.service.GetStartupStatusRequest;
import py.thrift.coordinator.service.GetStartupStatusResponse;
import py.thrift.coordinator.service.ResetSlowLevelRequest;
import py.thrift.coordinator.service.ResetSlowLevelResponse;
import py.thrift.coordinator.service.ShutdownRequest;
import py.thrift.coordinator.service.SlowDownRequest;
import py.thrift.coordinator.service.SlowDownResponse;
import py.thrift.coordinator.service.UpdateVolumeOnExtendingRequest;
import py.thrift.coordinator.service.UpdateVolumeOnExtendingResponse;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.ApplyVolumeAccessRulesToDriverRequest;
import py.thrift.share.ApplyVolumeAccessRulesToDriverResponse;
import py.thrift.share.CancelVolumeAccessRulesToDriverRequest;
import py.thrift.share.CancelVolumeAccessRulesToDriverResponse;
import py.thrift.share.GetConnectClientInfoRequest;
import py.thrift.share.GetConnectClientInfoResponse;
import py.thrift.share.GetDriverConnectPermissionRequestThrift;
import py.thrift.share.GetDriverConnectPermissionResponseThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.VolumeAccessRuleThrift;


public class CoordinatorImpl extends AbstractConfigurationServer implements Coordinator.Iface,
    ThriftProcessorFactory {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorImpl.class);

  private static final String STRING_LOG_LEVEL = "log.level";
  private static CoordinatorImpl instance = new CoordinatorImpl();
  private final Coordinator.Processor<Coordinator.Iface> processor;
  private CoordinatorAppEngine coordinatorAppEngine;
  private py.coordinator.lib.Coordinator coordinator;
  private IoLimitScheduler ioLimitScheduler;

  private AtomicBoolean shutdownThreadRunningFlag = new AtomicBoolean(false);
  private ExtendingVolumeUpdater extendingVolumeUpdater;

  private NbdConfiguration nbdConfiguration;


  private NbdServer nbdServer;

  private PydClientManager pydClientManager;

  private PeriodicWorkExecutor pullVolumeAccessRulesExecutor;
  private PeriodicWorkExecutor driverInfoReportExecutor;

  private CoordinatorImpl() {
    processor = new Coordinator.Processor<>(this);
  }

  public static CoordinatorImpl getInstance() {
    return instance;
  }



  public void initPeriodicWork() {
    PullVolumeAccessRulesWorkerFactory pullVolumeAccessRulesWorkerFactory =
        new PullVolumeAccessRulesWorkerFactory(
            coordinator.getInstanceStore(), nbdConfiguration, ioLimitScheduler);

    this.pullVolumeAccessRulesExecutor = new PeriodicWorkExecutorImpl(
        new ExecutionOptionsReader(1, 1,
            CoordinatorConfigSingleton.getInstance().getTimePullVolumeAccessRulesIntervalMs(),
            null),
        pullVolumeAccessRulesWorkerFactory);
    try {
      this.pullVolumeAccessRulesExecutor.start();
    } catch (UnableToStartException e) {
      logger.error("caught an exception when start pull volume access rules executor", e);
      throw new RuntimeException();
    }

    if (nbdConfiguration.getDriverType() == NBD) {
      logger.warn("start driver info reporter to driver container:{}",
          coordinator.getDriverContainerId().getId());

      DriverInfoReportWorkerFactory driverInfoReportWorkerFactory =
          new DriverInfoReportWorkerFactory(
              coordinator.getInstanceStore(), coordinator.getDriverContainerId(), nbdConfiguration,
              pydClientManager, coordinator.getVolumeInfoHolderManager());
      this.driverInfoReportExecutor = new PeriodicWorkExecutorImpl(new ExecutionOptionsReader(1, 1,
          CoordinatorConfigSingleton.getInstance().getReportDriverInfoIntervalTimeMs(), null),
          driverInfoReportWorkerFactory);

      try {
        this.driverInfoReportExecutor.start();
      } catch (UnableToStartException e) {
        logger.error("caught an exception when start report driver info executor", e);
        throw new RuntimeException();
      }
    } else {
      logger.warn("not start driver info reporter cause we start driver type:{}",
          nbdConfiguration.getDriverType());
    }
  }

  @Override
  public TProcessor getProcessor() {
    return processor;
  }

  @Override
  public void ping() throws TException {


  }



  public void setCoordinatorAppEngine(CoordinatorAppEngine coordinatorAppEngine) {
    coordinatorAppEngine
        .setMaxNetworkFrameSize(CoordinatorConfigSingleton.getInstance().getMaxNetworkFrameSize());
    this.coordinatorAppEngine = coordinatorAppEngine;
  }



  public void shutdown() {
    logger.warn("shutdown Coordinator now ");

    Thread shutdownProcessThread = new Thread(() -> {
      try {
        logger.warn("shut down thread try to close coordinator.");
        nbdServer.close();
        coordinator.close();
        coordinatorAppEngine.stop();
        extendingVolumeUpdater.close();
        pullVolumeAccessRulesExecutor.stop();
        if (nbdConfiguration.getDriverType() == NBD) {
          org.apache.commons.lang3.Validate.notNull(driverInfoReportExecutor);
          driverInfoReportExecutor.stop();
        }
      } catch (StorageException e) {
        logger.error("failed to stop coordinator thread", e);
      }
    }, "CoordinatorShutdownProcessThread");

    Thread shutdownWaitThread = new Thread(() -> {
      logger.info("wait coordinator shutdown");
      try {
        Thread.sleep(5000);
        logger.warn("finally exit coordinator");
        System.exit(0);
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
    }, "CoordinatorShutdownWaitThread");

    shutdownProcessThread.start();
    shutdownWaitThread.start();
  }

  @Override
  public void shutdown(ShutdownRequest request) throws ServiceHavingBeenShutdownThrift, TException {
    logger.warn("Shutting down coordinator now");
    if (shutdownThreadRunningFlag.compareAndSet(false, true) == false) {
      logger.error("service is shutting down now, no need to shutdown again");
      throw new ServiceHavingBeenShutdownThrift();
    }

    CoordinatorConfigSingleton.getInstance().setShutdown(true);

    if (request.isGraceful()) {
      GracefulShutdownCoordinatorWorkerFactory gracefulShutdownCoordinatorWorkerFactory =
          new GracefulShutdownCoordinatorWorkerFactory(
              this);

      PeriodicWorkExecutor gracefulShutdownExecutor = new PeriodicWorkExecutorImpl(
          new ExecutionOptionsReader(1, 1, null, 1000), gracefulShutdownCoordinatorWorkerFactory);

      try {
        gracefulShutdownExecutor.start();
        logger.warn("gracefully shutdown down coordinator");
      } catch (UnableToStartException e) {
        logger.error(
            "caught an exception when start GracefulShutdownCoordinatorWorkerFactory executor", e);
      }
    } else {
      logger.warn("Not gracefully shutdown down coordinator");
      shutdown();
    }
  }

  @Override
  public UpdateVolumeOnExtendingResponse updateVolumeOnExtending(
      UpdateVolumeOnExtendingRequest request)
      throws TException {
    logger.warn("{}", request);

    if (shutdownThreadRunningFlag.get()) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    extendingVolumeUpdater.addNewRequest(request);
    return new UpdateVolumeOnExtendingResponse(request.getRequestId());
  }

  public void setExtendingVolumeUpdater(ExtendingVolumeUpdater extendingVolumeUpdater) {
    this.extendingVolumeUpdater = extendingVolumeUpdater;
  }

  public void setNbdServer(NbdServer nbdServer) {
    this.nbdServer = nbdServer;
  }

  @Override
  public SlowDownResponse slowDownExceptFor(SlowDownRequest request) throws TException {
    logger.debug("slow down request {} ", request);
    try {
      ioLimitScheduler.slowDownExceptFor(request.getVolumeId(), request.getSlowDownLevel());
    } catch (Exception e) {
      logger.error("exception catch", e);
    }
    return new SlowDownResponse();
  }

  @Override
  public ResetSlowLevelResponse resetSlowLevel(ResetSlowLevelRequest request) throws TException {
    ioLimitScheduler.resetSlowLevel(request.getVolumeId());
    return new ResetSlowLevelResponse();
  }

  public void setCoordinator(py.coordinator.lib.Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  public PydClientManager getPydClientManager() {
    return pydClientManager;
  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
    this.extendingVolumeUpdater.setPydClientManager(this.pydClientManager);
  }

  @Override
  public ApplyVolumeAccessRulesToDriverResponse applyVolumeAccessRules(
      ApplyVolumeAccessRulesToDriverRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdownThreadRunningFlag.get()) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    ApplyVolumeAccessRulesToDriverResponse response = new ApplyVolumeAccessRulesToDriverResponse();
    response.setRequestId(request.getRequestId());

    if (!request.isSetApplyVolumeAccessRules() || request.getApplyVolumeAccessRules().isEmpty()) {
      logger.warn("apply volume access rules is not set or empty");
      return response;
    }

    for (VolumeAccessRuleThrift volumeAccessRuleThrift : request.getApplyVolumeAccessRules()) {
      AccessPermissionType access = AccessPermissionType
          .findByValue(volumeAccessRuleThrift.getPermission().getValue());
      logger.warn("apply volume access rule:{} for:{}", access,
          volumeAccessRuleThrift.getIncomingHostName());
      nbdConfiguration.getVolumeAccessRules()
          .put(volumeAccessRuleThrift.getIncomingHostName(), access);
    }

    return response;
  }

  @Override
  public CancelVolumeAccessRulesToDriverResponse cancelVolumeAccessRules(
      CancelVolumeAccessRulesToDriverRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdownThreadRunningFlag.get()) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    CancelVolumeAccessRulesToDriverResponse response =
        new CancelVolumeAccessRulesToDriverResponse();
    response.setRequestId(request.getRequestId());

    if (!request.isSetCancelVolumeAccessRules() || request.getCancelVolumeAccessRules().isEmpty()) {
      logger.warn("cancel volume access rules is not set or empty");
      return response;
    }

    for (VolumeAccessRuleThrift volumeAccessRuleThrift : request.getCancelVolumeAccessRules()) {
      AccessPermissionType access = AccessPermissionType
          .findByValue(volumeAccessRuleThrift.getPermission().getValue());
      logger.warn("going to cancel volume access rule:{} for:{}", access,
          volumeAccessRuleThrift.getIncomingHostName());
      AccessPermissionType removedAccess = nbdConfiguration.getVolumeAccessRules()
          .remove(volumeAccessRuleThrift.getIncomingHostName());
      if (!removedAccess.equals(access)) {
        logger.warn("current volume access rule:{} is not same with cancel rule:{}", removedAccess,
            access);
      }
    }

    return response;
  }

  @Override
  public GetConnectClientInfoResponse getConnectClientInfo(GetConnectClientInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdownThreadRunningFlag.get()) {
      logger.warn("receive request:{}, but coordinator server is shutting down now", request);
      throw new ServiceHavingBeenShutdownThrift();
    }
    logger.info("get connect client info request:{}", request);
    GetConnectClientInfoResponse response = new GetConnectClientInfoResponse();
    response.setRequestId(request.getRequestId());

    Map<String, AccessPermissionTypeThrift> connectClientAndAccessType = new HashMap<>();
    Map<SocketAddress, PydClientManager.ClientInfo> clientInfoMap = pydClientManager
        .getClientInfoMap();
    org.apache.commons.lang3.Validate.notNull(clientInfoMap);
    if (!clientInfoMap.isEmpty()) {
      Iterator<Map.Entry<SocketAddress, PydClientManager.ClientInfo>> iterator = clientInfoMap
          .entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Map.Entry<SocketAddress, PydClientManager.ClientInfo> entry = iterator.next();
        SocketAddress connectHostName = entry.getKey();
        AccessPermissionTypeThrift accessPermissionTypeThrift = AccessPermissionTypeThrift
            .valueOf(entry.getValue().getAccessPermissionType().name());
        logger.info("get connect client:{} and its' permission type:{}", connectHostName,
            accessPermissionTypeThrift);
        connectClientAndAccessType.put(connectHostName.toString(), accessPermissionTypeThrift);
      }
    }

    response.setConnectClientAndAccessType(connectClientAndAccessType);
    return response;
  }

  @Override
  public GetDriverConnectPermissionResponseThrift getDriverConnectPermission(
      GetDriverConnectPermissionRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdownThreadRunningFlag.get()) {
      logger.warn("receive request:{}, but coordinator server is shutting down now", request);
      throw new ServiceHavingBeenShutdownThrift();
    }
    logger.warn("{}", request);

    Iterator<Map.Entry<String, AccessPermissionType>> iterator = nbdConfiguration
        .getVolumeAccessRules().entrySet()
        .iterator();
    Map<String, AccessPermissionTypeThrift> connectPermissionMap = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, AccessPermissionType> entry = iterator.next();
      connectPermissionMap
          .put(entry.getKey(), AccessPermissionTypeThrift.valueOf(entry.getValue().name()));
    }

    GetDriverConnectPermissionResponseThrift responseThrift =
        new GetDriverConnectPermissionResponseThrift();
    responseThrift.setRequestId(request.getRequestId());
    responseThrift.setConnectPermissionMap(connectPermissionMap);
    logger.warn("get driver connect permission response:{}", responseThrift);
    return responseThrift;
  }

  @Override
  public GetStartupStatusResponse getStartupStatus(GetStartupStatusRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdownThreadRunningFlag.get()) {
      logger.warn("receive request:{}, but coordinator server is shutting down now", request);
      throw new ServiceHavingBeenShutdownThrift();
    }
    GetStartupStatusResponse response = new GetStartupStatusResponse();
    response.setRequestId(request.getRequestId());
    boolean startupStatus = coordinator.getStartupStatusByVolumeId(request.getVolumeId());
    response.setStartupStatus(startupStatus);
    return response;
  }

  public void setIoLimitScheduler(IoLimitScheduler ioLimitScheduler) {
    this.ioLimitScheduler = ioLimitScheduler;
  }


  @Override
  public AddOrModifyLimitationResponse addOrModifyLimitation(AddOrModifyLimitationRequest request)
      throws TException {
    logger.warn("receive add io limitation request:{}", request);
    ioLimitScheduler
        .addOrModifyIoLimitation(
            RequestResponseHelper.buildIoLimitationFrom(request.getIoLimitation()));
    return new AddOrModifyLimitationResponse(request.getRequestId());
  }

  @Override
  public DeleteLimitationResponse deleteLimitation(DeleteLimitationRequest request)
      throws TException {
    logger.warn("receive delete io limitation request:{}", request);
    ioLimitScheduler.deleteIoLimitation(request.getIoLimitationId());
    return new DeleteLimitationResponse();
  }

  public void setNbdConfiguration(NbdConfiguration nbdConfiguration) {
    this.nbdConfiguration = nbdConfiguration;
  }

}
