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

package py.coordinator.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.app.NetworkConfiguration;
import py.app.context.InstanceIdFileStore;
import py.app.healthcheck.HealthCheckerWithThriftImpl;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.coordinator.CoordinatorAppEngine;
import py.coordinator.CoordinatorBuilder;
import py.coordinator.driver.Driver;
import py.coordinator.driver.DriverExecutorService;
import py.coordinator.driver.DriverFactory;
import py.coordinator.nbd.IoLimitManagerImpl;
import py.coordinator.service.CoordinatorImpl;
import py.coordinator.worker.ExtendingVolumeUpdater;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.DriverAppContext;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.DriverTypeNotSupportedException;
import py.icshare.DriverKey;
import py.icshare.qos.IoLimitScheduler;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.monitor.jmx.configuration.JmxAgentConfiguration;
import py.thrift.datanode.service.DataNodeService;


@Configuration
@Import({LaunchDriverParameters.class, NetworkConfiguration.class,
    JmxAgentConfiguration.class, CoordinatorConfig.class})
public class CoordinatorServiceConfig {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorServiceConfig.class);

  @Autowired
  private NetworkConfiguration networkConfiguration;

  @Autowired
  private LaunchDriverParameters launchDriverParameters;

  @Autowired
  private JmxAgentConfiguration jmxAgentConfiguration;

  @Autowired
  private CoordinatorConfig coordinatorConfig;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }


  
  @Bean
  public CoordinatorBuilder coordinatorBuilder() throws Exception {
    CoordinatorBuilder coordinatorBuilder = new CoordinatorBuilder();
    coordinatorBuilder.setAccountId(launchDriverParameters.getAccountId());
    coordinatorBuilder.setVolumeId(launchDriverParameters.getVolumeId());
    coordinatorBuilder.setSnapshotId(launchDriverParameters.getSnapshotId());
    coordinatorBuilder.setInstanceId(launchDriverParameters.getDriverContainerInstanceId());

    coordinatorBuilder.setInformationCenterClientFactory(informationCenterClientFactory());
    coordinatorBuilder.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory());
    coordinatorBuilder.setDataNodeSyncClientFactory(dataNodeSyncClientFactory());
    coordinatorBuilder.setInstanceStore(instanceStore());

    return coordinatorBuilder;
  }


  
  @Bean
  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory factory = new InformationCenterClientFactory(1);
    factory.setInstanceName(PyService.INFOCENTER.getServiceName());
    factory.setInstanceStore(instanceStore());
    return factory;
  }

  @Bean
  public CoordinatorClientFactory coordinatorClientFactory() throws Exception {
    CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(1);
    return coordinatorClientFactory;
  }


  
  @Bean
  public ExtendingVolumeUpdater extendingVolumeUpdater() throws Exception {
    ExtendingVolumeUpdater extendingVolumeUpdater = new ExtendingVolumeUpdater(
        informationCenterClientFactory(), launchDriverParameters);
    extendingVolumeUpdater.start();
    coordinatorBuilder().setExtendingVolumeUpdater(extendingVolumeUpdater);
    return extendingVolumeUpdater;
  }


  
  @Bean
  public DriverFactory driverFactory() throws Exception {
    DriverFactory driverFactory = new DriverFactory(launchDriverParameters);
    CoordinatorBuilder builder = coordinatorBuilder();
    driverFactory.setCoordinatorBuilder(builder);
    driverFactory.setInformationCenterClientFactory(builder.getInformationCenterClientFactory());
    driverFactory.setIoLimitScheduler(ioLimitScheduler());
    return driverFactory;
  }


  
  @Bean
  public Driver driver() throws DriverTypeNotSupportedException, Exception {
   
    return driverFactory()
        .build(Driver.Type.valueOf(launchDriverParameters.getDriverType().name()),
           coordinator(), appContext());
  }


  
  @Bean
  public DriverExecutorService driverExecutorService() throws Exception {
    DriverExecutorService driverExecutorService = new DriverExecutorService();
    driverExecutorService.setDriver(driver());
    driverExecutorService.setLaunchDriverTimeoutMs(coordinatorConfig.getLaunchDriverTimeoutMs());
    return driverExecutorService;
  }

  @Bean
  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    return dihClientFactory;
  }


  
  @Bean
  public CoordinatorAppEngine coordinatorAppEngine() throws Exception {
    CoordinatorAppEngine coordinatorAppEngine = new CoordinatorAppEngine(coordinator());
    coordinatorAppEngine.setContext(appContext());
    coordinatorAppEngine.setHealthChecker(healthChecker());
    coordinatorAppEngine.start();
    return coordinatorAppEngine;
  }


  
  @Bean
  public CoordinatorImpl coordinator() throws Exception {
    CoordinatorImpl coordinatorImpl = CoordinatorImpl.getInstance();
    coordinatorImpl.setExtendingVolumeUpdater(extendingVolumeUpdater());
    coordinatorImpl.setIoLimitScheduler(ioLimitScheduler());
    return coordinatorImpl;
  }


  
  @Bean
  public IoLimitManagerImpl ioLimitManager() throws Exception {
    IoLimitManagerImpl ioLimitManager = new IoLimitManagerImpl(launchDriverParameters);
    ioLimitManager.setInstanceStore(instanceStore());
    ioLimitManager.setCoordinatorClientFactory(coordinatorClientFactory());
    return ioLimitManager;
  }


  
  @Bean
  public IoLimitScheduler ioLimitScheduler() throws Exception {
    IoLimitScheduler ioLimitScheduler = new IoLimitScheduler(ioLimitManager());
    ioLimitScheduler.setInformationCenterClientFactory(informationCenterClientFactory());
    DriverKey driverKey = new DriverKey(launchDriverParameters.getDriverContainerInstanceId(),
        launchDriverParameters.getVolumeId(), launchDriverParameters.getSnapshotId(),
        launchDriverParameters.getDriverType());
    ioLimitScheduler.setDriverKey(driverKey);
    return ioLimitScheduler;
  }


  
  @Bean
  public InstanceStore instanceStore() throws Exception {
    Object instanceStore = DihInstanceStore.getSingleton();
    ((DihInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
    ((DihInstanceStore) instanceStore).setDihEndPoint(localDihEP());
    ((DihInstanceStore) instanceStore).init();
    return (InstanceStore) instanceStore;
  }


  
  @Bean
  public EndPoint localDihEP() {
    return EndPointParser
        .parseLocalEndPoint(CoordinatorConfigSingleton.getInstance().getLocalDihPort(),
            appContext().getMainEndPoint().getHostName());
  }


  
  @Bean
  public DriverAppContext appContext() {
    String appName = CoordinatorConfigSingleton.getInstance().getAppName();
    logger.warn("parameters: {}, app name:{}", launchDriverParameters, appName);
    DriverAppContext appContext = new DriverAppContext(appName);
    appContext.setLocation(coordinatorConfig.getAppLocation());

    // control stream
    EndPoint endpointOfControlStream = EndPointParser
        .parseInSubnet(launchDriverParameters.getCoordinatorPort(),
            networkConfiguration.getControlFlowSubnet());
    appContext.putEndPoint(PortType.CONTROL, endpointOfControlStream);

    // // data stream
    EndPoint endpointOfDataStream = null;
    if (networkConfiguration.isEnableDataDepartFromControl()) {
      endpointOfDataStream = EndPointParser
          .parseInSubnet(launchDriverParameters.getCoordinatorPort(),
              networkConfiguration.getOutwardFlowSubnet());
    } else {
      endpointOfDataStream = EndPointParser
          .parseInSubnet(launchDriverParameters.getCoordinatorPort(),
              networkConfiguration.getControlFlowSubnet());
    }
    appContext.putEndPoint(PortType.IO, endpointOfDataStream);

    // monitor stream
    EndPoint endpointOfMonitorStream = EndPointParser
        .parseInSubnet(launchDriverParameters.getCoordinatorPort() + 100,
            networkConfiguration.getMonitorFlowSubnet());
    appContext.putEndPoint(PortType.MONITOR, endpointOfMonitorStream);

    appContext.setInstanceIdStore(
        new InstanceIdFileStore(appName, appName, endpointOfControlStream.getPort()));
    return appContext;
  }


  
  @Bean
  public GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.Iface.class)
        .withMaxChannelPendingSizeMb(
            CoordinatorConfigSingleton.getInstance().getMaxChannelPendingSize());
  }


  
  @Bean
  public GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.AsyncIface.class)
        .withMaxChannelPendingSizeMb(
            CoordinatorConfigSingleton.getInstance().getMaxChannelPendingSize());
  }


  
  @Bean
  public HealthCheckerWithThriftImpl healthChecker() {
    DriverAppContext appContext = appContext();
    logger.debug("Going to create HealthChecker by appContext:{}", appContext);

    HealthCheckerWithThriftImpl healthChecker = new HealthCheckerWithThriftImpl(
        CoordinatorConfigSingleton.getInstance().getHealthCheckerRate(), appContext());
    healthChecker.setServiceClientClazz(py.thrift.coordinator.service.Coordinator.Iface.class);
    healthChecker.setHeartBeatWorkerFactory(heartBeatWorkerFactory());
    return healthChecker;
  }


  
  @Bean
  public HeartBeatWorkerFactory heartBeatWorkerFactory() {
    DriverAppContext appContext = appContext();
    logger.debug("Going to create HeartBeatWorkerFactory by appContext:{}", appContext);

    HeartBeatWorkerFactory heartBeatWorkerFactory = new HeartBeatWorkerFactory();
    heartBeatWorkerFactory
        .setRequestTimeout(CoordinatorConfigSingleton.getInstance().getThriftRequestTimeoutMs());
    heartBeatWorkerFactory.setLocalDihEndPoint(localDihEP());
    heartBeatWorkerFactory.setAppContext(appContext);
    heartBeatWorkerFactory.setDihClientFactory(dihClientFactory());
    return heartBeatWorkerFactory;
  }

}
