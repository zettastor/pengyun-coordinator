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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.app.NetworkConfiguration;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverType;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.client.InformationCenterClientFactory;
import py.informationcenter.AccessPermissionType;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.GetVolumeAccessRulesRequest;
import py.thrift.share.GetVolumeAccessRulesResponse;
import py.thrift.share.VolumeAccessRuleThrift;


@Configuration
@Import({NetworkConfiguration.class})
@PropertySource({"classpath:config/nbd-server.properties"})
public class NbdConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(NbdConfiguration.class);

  @Value("${enable.heartbeat.handler:true}")
  private boolean enableHeartbeatHandler = true;

  @Value("${reader.idle.timeout.sec:30}")
  private int readerIdleTimeoutSec = 30;

  @Value("${writer.idle.timeout.sec:30}")
  private int writerIdleTimeoutSec = 30;

  @Value("${all.idle.timeout.sec:30}")
  private int allIdleTimeoutSec = 30;

  @Value("${heartbeat.time.interval.after.io.request.ms:0}")
  private int heartbeatTimeIntervalAfterIoRequestMs = 0;

  @Value("${enable.connection.limit:true}")
  private boolean enableClientConnectionLimit = true;

  @Value("${ndb.max.buf.length.for.allocate.adapter:2097152}")
  private int nbdMaxBufLengthForAllocateAdapter = 2097152;
  @Value("${nbd.server.io.event.group.threads.mode:0}")
  private int nbdServerIoEventGroupThreadsMode = 0;
  @Value("${nbd.server.io.event.group.threads.parameter:2}")
  private float nbdServerIoEventGroupThreadsParameter = 2;
  @Value("${nbd.server.io.event.handle.threads.mode:0}")
  private int nbdServerIoEventHandleThreadsMode = 0;
  @Value("${nbd.server.io.event.handle.threads.parameter:4}")
  private float nbdServerIoEventHandleThreadsParameter = 4;


  @Autowired
  private NetworkConfiguration networkConfiguration;

  private EndPoint endpoint;

  /*
   * An information center client builder which build a client connect to infocenter node to get
   * some info from it.
   */
  private InformationCenterClientFactory informationCenterClientFactory;

  /*
   * The id for backend volume binding with the nbd driver.
   */
  private long volumeId;

  private int snapshotId;

  private DriverType driverType;

  private long driverContainerId;

  private long driverId;

  /**
   * Info in this table manage access to the backend volume. All type of access to the volume
   * recorded in {@link AccessPermissionType}.
   */
  private Map<String, AccessPermissionType> volumeAccessRuleTable = new ConcurrentHashMap<>();



  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public boolean isEnableHeartbeatHandler() {
    return enableHeartbeatHandler;
  }

  public int getReaderIdleTimeoutSec() {
    return readerIdleTimeoutSec;
  }

  public int getWriterIdleTimeoutSec() {
    return writerIdleTimeoutSec;
  }

  public int getAllIdleTimeoutSec() {
    return allIdleTimeoutSec;
  }

  public boolean isEnableClientConnectionLimit() {
    return enableClientConnectionLimit;
  }

  public NetworkConfiguration getNetworkConfiguration() {
    return networkConfiguration;
  }

  public void setNetworkConfiguration(NetworkConfiguration networkConfiguration) {
    this.networkConfiguration = networkConfiguration;
  }

  public EndPoint getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(EndPoint endpoint) {
    this.endpoint = endpoint;
  }

  public int getHeartbeatTimeIntervalAfterIoRequestMs() {
    return heartbeatTimeIntervalAfterIoRequestMs;
  }

  public void setHeartbeatTimeIntervalAfterIoRequestMs(int heartbeatTimeIntervalAfterIoRequestMs) {
    this.heartbeatTimeIntervalAfterIoRequestMs = heartbeatTimeIntervalAfterIoRequestMs;
  }

  public DriverType getDriverType() {
    return driverType;
  }

  public void setDriverType(DriverType driverType) {
    this.driverType = driverType;
  }

  public long getDriverContainerId() {
    return driverContainerId;
  }

  public void setDriverContainerId(long driverContainerId) {
    this.driverContainerId = driverContainerId;
  }

  public long getDriverId() {
    return driverId;
  }

  public void setDriverId(long driverId) {
    this.driverId = driverId;
  }

  /**
   * Query access rules to manage connection access and read & write access.
   *
   * @return a map from remote client ip to access permission.
   */
  public Map<String, AccessPermissionType> getVolumeAccessRulesFromInfoCenter(long volumeId)
      throws VolumeNotFoundException {
    try {
      InformationCenter.Iface icClient = informationCenterClientFactory.build().getClient();
      GetVolumeAccessRulesRequest rules = new GetVolumeAccessRulesRequest(RequestIdBuilder.get(),
          0L, volumeId);
      GetVolumeAccessRulesResponse response = icClient.getVolumeAccessRules(rules);

      synchronized (volumeAccessRuleTable) {
        volumeAccessRuleTable.clear();

        if (response.getAccessRules() != null && response.getAccessRulesSize() > 0) {
          for (VolumeAccessRuleThrift rule : response.getAccessRules()) {
            AccessPermissionType access = AccessPermissionType
                .findByValue(rule.getPermission().getValue());
            volumeAccessRuleTable.put(rule.getIncomingHostName(), access);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Caught an exception when get access rules for volume {}", volumeId, e);
      throw new VolumeNotFoundException();
    }
    return volumeAccessRuleTable;
  }

  public Map<String, AccessPermissionType> getVolumeAccessRules() {
    return volumeAccessRuleTable;
  }

  public void setVolumeAccessRuleTable(Map<String, AccessPermissionType> volumeAccessRuleTable) {
    this.volumeAccessRuleTable = volumeAccessRuleTable;
  }

  public InformationCenterClientFactory getInformationCenterClientFactory() {
    return informationCenterClientFactory;
  }

  public void setInformationCenterClientFactory(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  @Override
  public String toString() {
    return "NBDConfiguration{"
        + "enableHeartbeatHandler=" + enableHeartbeatHandler
        + ", readerIdleTimeoutSec=" + readerIdleTimeoutSec
        + ", writerIdleTimeoutSec=" + writerIdleTimeoutSec
        + ", allIdleTimeoutSec=" + allIdleTimeoutSec
        + ", heartbeatTimeIntervalAfterIORequestMs=" + heartbeatTimeIntervalAfterIoRequestMs
        + ", enableClientConnectionLimit=" + enableClientConnectionLimit
        + ", networkConfiguration=" + networkConfiguration
        + ", endpoint=" + endpoint
        + ", informationCenterClientFactory=" + informationCenterClientFactory
        + ", volumeId=" + volumeId
        + ", snapshotId=" + snapshotId
        + ", driverType=" + driverType
        + ", driverContainerId=" + driverContainerId
        + ", driverId=" + driverId
        + ", volumeAccessRuleTable=" + volumeAccessRuleTable
        + '}';
  }

  public int getNbdMaxBufLengthForAllocateAdapter() {
    return nbdMaxBufLengthForAllocateAdapter;
  }

  public void setNbdMaxBufLengthForAllocateAdapter(int nbdMaxBufLengthForAllocateAdapter) {
    this.nbdMaxBufLengthForAllocateAdapter = nbdMaxBufLengthForAllocateAdapter;
  }

  public int getNbdServerIoEventGroupThreadsMode() {
    return nbdServerIoEventGroupThreadsMode;
  }

  public void setNbdServerIoEventGroupThreadsMode(int nbdServerIoEventGroupThreadsMode) {
    this.nbdServerIoEventGroupThreadsMode = nbdServerIoEventGroupThreadsMode;
  }

  public float getNbdServerIoEventGroupThreadsParameter() {
    return nbdServerIoEventGroupThreadsParameter;
  }

  public void setNbdServerIoEventGroupThreadsParameter(
      float nbdServerIoEventGroupThreadsParameter) {
    this.nbdServerIoEventGroupThreadsParameter = nbdServerIoEventGroupThreadsParameter;
  }

  public int getNbdServerIoEventHandleThreadsMode() {
    return nbdServerIoEventHandleThreadsMode;
  }

  public void setNbdServerIoEventHandleThreadsMode(int nbdServerIoEventHandleThreadsMode) {
    this.nbdServerIoEventHandleThreadsMode = nbdServerIoEventHandleThreadsMode;
  }

  public float getNbdServerIoEventHandleThreadsParameter() {
    return nbdServerIoEventHandleThreadsParameter;
  }

  public void setNbdServerIoEventHandleThreadsParameter(
      float nbdServerIoEventHandleThreadsParameter) {
    this.nbdServerIoEventHandleThreadsParameter = nbdServerIoEventHandleThreadsParameter;
  }
}
