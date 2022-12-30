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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.app.NettyConfiguration;
import py.app.NetworkConfiguration;
import py.netty.core.IoEventThreadsMode;
import py.storage.StorageConfiguration;

@Configuration
@PropertySource("classpath:config/coordinator.properties")
@Import({StorageConfiguration.class, NetworkConfiguration.class, NettyConfiguration.class})
public class CoordinatorConfig {

  @Value("${app.name}")
  private String appname;

  @Value("${app.location}")
  private String appLocation;

  @Value("${health.checker.rate}")
  private int healthCheckerRate;

  @Value("${local.dih.port}")
  private int localDihPort;

  @Value("${thrift.request.timeout.ms:30000}")
  private int thriftRequestTimeoutMs = 30000;

  @Value("${thrift.connect.timeout.ms:20000}")
  private int thriftConnectTimeoutMs = 20000;

  @Value("${netty.request.timeout.ms:10000}")
  private int nettyRequestTimeoutMs = 10000;

  @Value("${netty.connect.timeout.ms:5000}")
  private int nettyConnectTimeoutMs = 5000;

  
  @Value("${max.write.data.size.per.request:1048576}")
  private int maxWriteDataSizePerRequest = 1048576;

  @Value("${max.read.data.size.per.request:1048576}")
  private int maxReadDataSizePerRequest = 1048576;

  @Value("${page.wrapped.count:128}")
  private int pageWrappedCount = 128;

  @Value("${send.log.result.after.no.request.ms:2000}")
  private int sendLogResultAfterNoRequestMs = 2000;

  @Value("${log.level}")
  private String logLevel;

  @Value("${log.output.file}")
  private String logOutputFile;

  @Value("${page.cache.for.read:0}")
  private int pageCacheForRead = 0;

  @Value("${read.cache.for.io: 0}")
  private int readCacheForIO = 0;

  @Value("${netty4.connection.pool.size:2}")
  private int connectionPoolSize = 2;

  @Value("${resend.delay.time.unit.ms:20}")
  private int resendDelayTimeUnitMs = 20;

  @Value("${convert.position.type:stripe}")
  private String convertPosType;

  @Value("${io.depth:128}")
  private int ioDepth = 128;

  @Value("${data.storage.path}")
  private String dataStoragePath;

  @Value("${data.storage.size.gb}")
  private int dataStorageSizeGB;

  @Value("${read.write.log.flag:false}")
  private boolean rwLogFlag = false;

  @Value("${large.page.size.for.pool:131072}")
  private int largePageSizeForPool = 131072;

  @Value("${medium.page.size.for.pool:8192}")
  private int mediumPageSizeForPool = 8192;

  @Value("${little.page.size.for.pool:1024}")
  private int littlePageSizeForPool = 1024;

  @Value("${cache.pool.size.for.network:256M}")
  private String cachePoolSize = "256M";

  @Value("${max.channel.pending.size.mb:256M}")
  private int maxChannelPendingSize = 256;

  
  @Value("${network.checksum.algorithm:CRC32}")
  private String networkChecksumAlgorithm = "CRC32";

  @Value("${enable.process.ssd:false}")
  private boolean enbaleProcessSsd = false;

  @Value("${ping.host.timeout.ms:100}")
  private int pingHostTimeoutMs = 100;

  @Value("${network.connection.detect.retry.maxtimes:3}")
  private int networkConnectionDetectRetryMaxtimes = 3;

  @Value("${debug.io.timeout.threshold.ms:1000}")
  private int debugIoTimeoutMsThreshold = 1000;

  @Value("${stream.io:false}")
  private boolean streamIO = false;


  @Value("${enable.logger.tracer:false}")
  private boolean enableLoggerTracer = false;

  @Value("${trace.all.logs:true}")
  private boolean traceAllLogs = true;

  @Value("${time.pull.volume.access.rules.interval.ms:1000}")
  private int timePullVolumeAccessRulesIntervalMs = 1000;


  @Value("${report.driver.info.interval.time.ms:1000}")
  private int reportDriverInfoIntervalTimeMs = 1000;

  @Value("${launch.driver.timeout.ms:100000}")
  private int launchDriverTimeoutMs = 100000;

  @Value("${try.send.io.semaphore.timeout.ms:100}")
  private int trySendIoSemaphoreTimeoutMs = 100;

  @Value("${try.semaphore.max.count.per.time:32}")
  private int trySemaphoreMaxCountPerTime = 32;

  @Value("${discard.log.max.count.per.request:4096}")
  private int discardLogMaxCountPerRequest = 4096;

  @Autowired
  private StorageConfiguration storageConfiguration;

  @Autowired
  private NetworkConfiguration networkConfiguration;

  @Autowired
  private NettyConfiguration nettyConfiguration;

 
 
  @Value("${network.connection.detect.server.listening.port:54321}")
  private int networkConnectionDetectServerListeningPort = 54321;

  @Value("${max.network.frame.size:17000000}")
  private int maxNetworkFrameSize = 17 * 1000 * 1000;


  @Value("${network.healthy.check.time:10}")
  private int networkHealthyChecktime = 10;

  @Value("${performance.file.path:/opt/performance}")
  private String performanceFilePath = "/opt/performance";

  @Value("${network.delay.record.time.window.second:15}")
  private int networkDelayRecordTimeWindowSec = 15;

  @Value("${network.delay.mean.output.interval.second:5}")
  private int networkDelayMeanOutputIntervalSec = 5;


  @Value("${io.log.sequential.condition:8}")
  private int ioLogSequentialCondition = 8;


  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  public int getNetworkHealthyChecktime() {
    return networkHealthyChecktime;
  }

  public void setNetworkHealthyChecktime(int networkHealthyChecktime) {
    this.networkHealthyChecktime = networkHealthyChecktime;
  }

  public int getHealthCheckerRate() {
    return healthCheckerRate;
  }

  public void setHealthCheckerRate(int healthCheckerRate) {
    this.healthCheckerRate = healthCheckerRate;
  }

  public int getLaunchDriverTimeoutMs() {
    return launchDriverTimeoutMs;
  }

  public String getAppLocation() {
    return appLocation;
  }

  public void setAppLocation(String appLocation) {
    this.appLocation = appLocation;
  }


  
  @Bean
  public CoordinatorConfigSingleton coordinatorConfiguration() {

    CoordinatorConfigSingleton coordinatorConfigSingleton = CoordinatorConfigSingleton
        .getInstance();
    coordinatorConfigSingleton.setThriftRequestTimeoutMs(thriftRequestTimeoutMs);
    coordinatorConfigSingleton.setThriftConnectTimeoutMs(thriftConnectTimeoutMs);
    coordinatorConfigSingleton.setNettyRequestTimeoutMs(nettyRequestTimeoutMs);
    coordinatorConfigSingleton.setNettyConnectTimeoutMs(nettyConnectTimeoutMs);
    coordinatorConfigSingleton.setWriteIoTimeoutMs(storageConfiguration.getIoTimeoutMs());
    coordinatorConfigSingleton.setReadIoTimeoutMs(storageConfiguration.getIoTimeoutMs());
    coordinatorConfigSingleton.setPingHostTimeoutMs(pingHostTimeoutMs);

    coordinatorConfigSingleton.setPageSize((int) storageConfiguration.getPageSizeByte());
    coordinatorConfigSingleton.setSegmentSize(storageConfiguration.getSegmentSizeByte());

    coordinatorConfigSingleton.setMaxWriteDataSizePerRequest(maxWriteDataSizePerRequest);
    coordinatorConfigSingleton.setMaxReadDataSizePerRequest(maxReadDataSizePerRequest);
    coordinatorConfigSingleton.setPageWrappedCount(pageWrappedCount);
    coordinatorConfigSingleton.setCommitLogsAfterNoWriteRequestMs(sendLogResultAfterNoRequestMs);
    coordinatorConfigSingleton.setPageCacheForRead(pageCacheForRead);
    coordinatorConfigSingleton.setReadCacheForIo(readCacheForIO);
    coordinatorConfigSingleton.setConnectionPoolSize(connectionPoolSize);
    coordinatorConfigSingleton.setResendDelayTimeUnitMs(resendDelayTimeUnitMs);
    coordinatorConfigSingleton.setConvertPosType(convertPosType);

    coordinatorConfigSingleton.setRwLogFlag(rwLogFlag);
    coordinatorConfigSingleton.setDataStoragePath(dataStoragePath);
    coordinatorConfigSingleton.setDataStorageSizeGB(dataStorageSizeGB);
    coordinatorConfigSingleton.setEnableProcessSsd(enbaleProcessSsd);
    coordinatorConfigSingleton.setLocalDihPort(localDihPort);

    coordinatorConfigSingleton.setIoDepth(ioDepth);
    coordinatorConfigSingleton.setLargePageSizeForPool(largePageSizeForPool);
    coordinatorConfigSingleton.setMediumPageSizeForPool(mediumPageSizeForPool);
    coordinatorConfigSingleton.setLittlePageSizeForPool(littlePageSizeForPool);
    coordinatorConfigSingleton.setCachePoolSize(cachePoolSize);
    coordinatorConfigSingleton.setMaxChannelPendingSize(maxChannelPendingSize);

    coordinatorConfigSingleton.setAppName(appname);
    coordinatorConfigSingleton.setHealthCheckerRate(healthCheckerRate);
    coordinatorConfigSingleton.setDebugIoTimeoutMsThreshold(debugIoTimeoutMsThreshold);
    coordinatorConfigSingleton.setStreamIo(streamIO);
    coordinatorConfigSingleton.setMaxNetworkFrameSize(maxNetworkFrameSize);
    coordinatorConfigSingleton.setEnableLoggerTracer(enableLoggerTracer);
    coordinatorConfigSingleton
        .setTimePullVolumeAccessRulesIntervalMs(timePullVolumeAccessRulesIntervalMs);
    coordinatorConfigSingleton.setReportDriverInfoIntervalTimeMs(reportDriverInfoIntervalTimeMs);
    coordinatorConfigSingleton.setTraceAllLogs(traceAllLogs);
    coordinatorConfigSingleton
        .setNetworkConnectionDetectServerListeningPort(networkConnectionDetectServerListeningPort);
    coordinatorConfigSingleton
        .setNetworkConnectionDetectRetryMaxtimes(networkConnectionDetectRetryMaxtimes);
    coordinatorConfigSingleton.setPerformanceFilePath(performanceFilePath);
    coordinatorConfigSingleton.setTrySendIoSemaphoreTimeoutMs(trySendIoSemaphoreTimeoutMs);
    coordinatorConfigSingleton.setTrySemaphoreMaxCountPerTime(trySemaphoreMaxCountPerTime);
    coordinatorConfigSingleton.setDiscardLogMaxCountPerRequest(discardLogMaxCountPerRequest);
    coordinatorConfigSingleton.setNetworkDelayRecordTimeWindowSec(networkDelayRecordTimeWindowSec);
    coordinatorConfigSingleton
        .setNetworkDelayMeanOutputIntervalSec(networkDelayMeanOutputIntervalSec);
    coordinatorConfigSingleton.setNetworkHealthyCheckTime(networkHealthyChecktime);
    coordinatorConfigSingleton.setIoLogSequentialCondition(ioLogSequentialCondition);

    coordinatorConfigSingleton.setMaxBufLengthForNettyAllocateAdapter(
        nettyConfiguration.getNettyMaxBufLengthForAllocateAdapter());

    coordinatorConfigSingleton.setIoEventGroupThreadsMode(IoEventThreadsMode
        .findByValue(nettyConfiguration.getNettyServerIoEventGroupThreadsMode()));
    coordinatorConfigSingleton.setIoEventGroupThreadsParameter(
        nettyConfiguration.getNettyServerIoEventGroupThreadsParameter());
    coordinatorConfigSingleton.setIoEventHandleThreadsMode(IoEventThreadsMode
        .findByValue(nettyConfiguration.getNettyServerIoEventHandleThreadsMode()));
    coordinatorConfigSingleton.setIoEventHandleThreadsParameter(
        nettyConfiguration.getNettyServerIoEventHandleThreadsParameter());

    coordinatorConfigSingleton.setClientIoEventGroupThreadsMode(IoEventThreadsMode
        .findByValue(nettyConfiguration.getNettyClientIoEventGroupThreadsMode()));
    coordinatorConfigSingleton.setClientIoEventGroupThreadsParameter(
        nettyConfiguration.getNettyClientIoEventGroupThreadsParameter());
    coordinatorConfigSingleton.setClientIoEventHandleThreadsMode(IoEventThreadsMode
        .findByValue(nettyConfiguration.getNettyClientIoEventHandleThreadsMode()));
    coordinatorConfigSingleton.setClientIoEventHandleThreadsParameter(
        nettyConfiguration.getNettyClientIoEventHandleThreadsParameter());

    return coordinatorConfigSingleton;
  }
}
