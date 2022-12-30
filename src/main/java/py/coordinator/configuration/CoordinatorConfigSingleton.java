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

import java.util.concurrent.atomic.AtomicBoolean;
import py.coordinator.calculator.LogicalToPhysicalCalculatorFactory;
import py.informationcenter.Utils;
import py.netty.core.IoEventThreadsMode;


public class CoordinatorConfigSingleton {

  private static CoordinatorConfigSingleton coordinatorConfigSingleton =
      new CoordinatorConfigSingleton();
  private final int discardStepLen = 2;
  private int writeIoTimeoutMs = 30000;
  private int readIoTimeoutMs = 30000;
  private int thriftRequestTimeoutMs = 30000;
  private int thriftConnectTimeoutMs = 20000;
  private int nettyRequestTimeoutMs = 10000;
  private int nettyConnectTimeoutMs = 5000;
  private int commitLogsAfterNoWriteRequestMs = 3000;
  private long segmentSize;
  private int pageSize;
  private int maxWriteDataSizePerRequest = 1024 * 1024;
  private int maxReadDataSizePerRequest = 1024 * 1024;
  private int pageWrappedCount = 128;
  private AtomicBoolean shutdown = new AtomicBoolean(false);
  private int pageCacheForRead = 4096;
  private int readCacheForIo = 1024;
  private int connectionPoolSize = 2;
  private int resendDelayTimeUnitMs = 20;
  private int ioDepth = 128;

  private boolean rwLogFlag = false;
  private String dataStoragePath;
  private int dataStorageSizeGB;

  private long lowerLimitIops = 30000L;
  private long lowerLimitThroughput = 500 * 1024L * 1024L;
  private int largePageSizeForPool = 128 * 1024;
  private int mediumPageSizeForPool = 8 * 1024;
  private int littlePageSizeForPool = 1024;
  private String cachePoolSize = "256M";
  private boolean enableProcessSsd = false;
  private String appName;
  private int healthCheckerRate;
  private int localDihPort;
  private int maxChannelPendingSize = 256;
  private int pingHostTimeoutMs = 50;
  private int networkConnectionDetectRetryMaxtimes = 5;
  private int debugIoTimeoutMsThreshold = 1000;
  private boolean streamIo = false;
  private boolean enableLoggerTracer = false;
  private String convertPosType = LogicalToPhysicalCalculatorFactory.ConvertPosType.STRIPE.name();
  private int maxNetworkFrameSize = 17 * 1000 * 1000;
  private int timePullVolumeAccessRulesIntervalMs = 1000;
  private int reportDriverInfoIntervalTimeMs = 1000;
  private boolean traceAllLogs = true;
  private int trySendIoSemaphoreTimeoutMs = 100;
  private int trySemaphoreMaxCountPerTime = 32;
  private int discardLogMaxCountPerRequest = 4096;
  private int networkConnectionDetectServerListeningPort = 54321;
  private String performanceFilePath = "/opt/performance";
  private int networkDelayRecordTimeWindowSec = 15;
  private int networkDelayMeanOutputIntervalSec = 5;
  private int networkHealthyCheckTime = 10;
  private int ioLogSequentialCondition = 8;

  private int maxBufLengthForNettyAllocateAdapter = 16 * 1024;
  private IoEventThreadsMode ioEventGroupThreadsMode = IoEventThreadsMode.Fix_Threads_Mode;
  private float ioEventGroupThreadsParameter = 2.0f;
  private IoEventThreadsMode ioEventHandleThreadsMode = IoEventThreadsMode.Fix_Threads_Mode;
  private float ioEventHandleThreadsParameter = 4.0f;

  private IoEventThreadsMode clientIoEventGroupThreadsMode = IoEventThreadsMode.Fix_Threads_Mode;
  private float clientIoEventGroupThreadsParameter = 2.0f;
  private IoEventThreadsMode clientIoEventHandleThreadsMode = IoEventThreadsMode.Fix_Threads_Mode;
  private float clientIoEventHandleThreadsParameter = 4.0f;

  private CoordinatorConfigSingleton() {
  }

  public static CoordinatorConfigSingleton getInstance() {
    return coordinatorConfigSingleton;
  }

  public int getNetworkConnectionDetectServerListeningPort() {
    return networkConnectionDetectServerListeningPort;
  }

  public void setNetworkConnectionDetectServerListeningPort(
      int networkConnectionDetectServerListeningPort) {
    this.networkConnectionDetectServerListeningPort = networkConnectionDetectServerListeningPort;
  }


  public int getMaxNetworkFrameSize() {
    return maxNetworkFrameSize;
  }

  public void setMaxNetworkFrameSize(int maxNetworkFrameSize) {
    this.maxNetworkFrameSize = maxNetworkFrameSize;
  }

  public int getIoDepth() {
    return ioDepth;
  }

  public void setIoDepth(int ioDepth) {
    this.ioDepth = ioDepth;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public int getMaxWriteDataSizePerRequest() {
    return maxWriteDataSizePerRequest;
  }

  public void setMaxWriteDataSizePerRequest(int maxWriteDataSizePerRequest) {
    this.maxWriteDataSizePerRequest = maxWriteDataSizePerRequest;
  }

  public int getMaxReadDataSizePerRequest() {
    return maxReadDataSizePerRequest;
  }

  public void setMaxReadDataSizePerRequest(int maxReadDataSizePerRequest) {
    this.maxReadDataSizePerRequest = maxReadDataSizePerRequest;
  }

  public int getPageWrappedCount() {
    return pageWrappedCount;
  }

  public void setPageWrappedCount(int pageWrappedCount) {
    this.pageWrappedCount = pageWrappedCount;
  }

  public int getCommitLogsAfterNoWriteRequestMs() {
    return commitLogsAfterNoWriteRequestMs;
  }

  public void setCommitLogsAfterNoWriteRequestMs(int commitLogsAfterNoWriteRequestMs) {
    this.commitLogsAfterNoWriteRequestMs = commitLogsAfterNoWriteRequestMs;
  }

  public boolean isShutdown() {
    return shutdown.get();
  }

  public void setShutdown(boolean shutdown) {
    this.shutdown.set(shutdown);
  }

  public int getPageCacheForRead() {
    return pageCacheForRead;
  }

  public void setPageCacheForRead(int pageCacheForRead) {
    this.pageCacheForRead = pageCacheForRead;
  }

  public int getReadCacheForIo() {
    return readCacheForIo;
  }

  public void setReadCacheForIo(int readCacheForIo) {
    this.readCacheForIo = readCacheForIo;
  }

  public int getConnectionPoolSize() {
    return connectionPoolSize;
  }

  public void setConnectionPoolSize(int connectionPoolSize) {
    this.connectionPoolSize = connectionPoolSize;
  }

  public int getResendDelayTimeUnitMs() {
    return resendDelayTimeUnitMs;
  }

  public void setResendDelayTimeUnitMs(int resendDelayTimeUnitMs) {
    this.resendDelayTimeUnitMs = resendDelayTimeUnitMs;
  }

  public String getConvertPosType() {
    return convertPosType;
  }

  public void setConvertPosType(String convertPosType) {
    this.convertPosType = convertPosType;
  }



  public int getNetworkHealthyCheckTime() {
    return networkHealthyCheckTime;
  }

  public void setNetworkHealthyCheckTime(int networkHealthyCheckTime) {
    this.networkHealthyCheckTime = networkHealthyCheckTime;
  }

  public boolean getRwLogFlag() {
    return rwLogFlag;
  }

  public void setRwLogFlag(boolean rwLogFlag) {
    this.rwLogFlag = rwLogFlag;
  }

  public String getDataStoragePath() {
    return dataStoragePath;
  }

  public void setDataStoragePath(String dataStoragePath) {
    this.dataStoragePath = dataStoragePath;
  }

  public int getDataStorageSizeGB() {
    return dataStorageSizeGB;
  }

  public void setDataStorageSizeGB(int dataStorageSizeGB) {
    this.dataStorageSizeGB = dataStorageSizeGB;
  }

  public long getLowerLimitIops() {
    return lowerLimitIops;
  }

  public void setLowerLimitIops(long lowerLimitIops) {
    this.lowerLimitIops = lowerLimitIops;
  }

  public long getLowerLimitThroughput() {
    return lowerLimitThroughput;
  }

  public void setLowerLimitThroughput(long lowerLimitThroughput) {
    this.lowerLimitThroughput = lowerLimitThroughput;
  }

  public int getWriteIoTimeoutMs() {
    return writeIoTimeoutMs;
  }

  public void setWriteIoTimeoutMs(int writeIoTimeoutMs) {
    this.writeIoTimeoutMs = writeIoTimeoutMs;
  }

  public int getReadIoTimeoutMs() {
    return readIoTimeoutMs;
  }

  public void setReadIoTimeoutMs(int readIoTimeoutMs) {
    this.readIoTimeoutMs = readIoTimeoutMs;
  }

  public int getThriftRequestTimeoutMs() {
    return thriftRequestTimeoutMs;
  }

  public void setThriftRequestTimeoutMs(int thriftRequestTimeoutMs) {
    this.thriftRequestTimeoutMs = thriftRequestTimeoutMs;
  }

  public int getThriftConnectTimeoutMs() {
    return thriftConnectTimeoutMs;
  }

  public void setThriftConnectTimeoutMs(int thriftConnectTimeoutMs) {
    this.thriftConnectTimeoutMs = thriftConnectTimeoutMs;
  }

  public int getNettyRequestTimeoutMs() {
    return nettyRequestTimeoutMs;
  }

  public void setNettyRequestTimeoutMs(int nettyRequestTimeoutMs) {
    this.nettyRequestTimeoutMs = nettyRequestTimeoutMs;
  }

  public int getNettyConnectTimeoutMs() {
    return nettyConnectTimeoutMs;
  }

  public void setNettyConnectTimeoutMs(int nettyConnectTimeoutMs) {
    this.nettyConnectTimeoutMs = nettyConnectTimeoutMs;
  }

  public int getLargePageSizeForPool() {
    return largePageSizeForPool;
  }

  public void setLargePageSizeForPool(int largePageSizeForPool) {
    this.largePageSizeForPool = largePageSizeForPool;
  }

  public int getMediumPageSizeForPool() {
    return mediumPageSizeForPool;
  }

  public void setMediumPageSizeForPool(int mediumPageSizeForPool) {
    this.mediumPageSizeForPool = mediumPageSizeForPool;
  }

  public int getLittlePageSizeForPool() {
    return littlePageSizeForPool;
  }

  public void setLittlePageSizeForPool(int littlePageSizeForPool) {
    this.littlePageSizeForPool = littlePageSizeForPool;
  }

  public String getCachePoolSize() {
    return cachePoolSize;
  }

  public void setCachePoolSize(String cachePoolSize) {
    this.cachePoolSize = cachePoolSize;
  }

  public long getCachePoolSizeBytes() {
    return Utils.getByteSize(cachePoolSize);
  }

  public boolean isEnableProcessSsd() {
    return enableProcessSsd;
  }

  public void setEnableProcessSsd(boolean enableProcessSsd) {
    this.enableProcessSsd = enableProcessSsd;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public int getHealthCheckerRate() {
    return healthCheckerRate;
  }

  public void setHealthCheckerRate(int healthCheckerRate) {
    this.healthCheckerRate = healthCheckerRate;
  }

  public int getLocalDihPort() {
    return localDihPort;
  }

  public void setLocalDihPort(int localDihPort) {
    this.localDihPort = localDihPort;
  }

  public int getMaxChannelPendingSize() {
    return maxChannelPendingSize;
  }

  public void setMaxChannelPendingSize(int maxChannelPendingSize) {
    this.maxChannelPendingSize = maxChannelPendingSize;
  }

  public int getPingHostTimeoutMs() {
    return pingHostTimeoutMs;
  }

  public void setPingHostTimeoutMs(int pingHostTimeoutMs) {
    this.pingHostTimeoutMs = pingHostTimeoutMs;
  }

  public int getNetworkConnectionDetectRetryMaxtimes() {
    return networkConnectionDetectRetryMaxtimes;
  }

  public void setNetworkConnectionDetectRetryMaxtimes(int networkConnectionDetectRetryMaxtimes) {
    this.networkConnectionDetectRetryMaxtimes = networkConnectionDetectRetryMaxtimes;
  }

  public int getDebugIoTimeoutMsThreshold() {
    return debugIoTimeoutMsThreshold;
  }

  public void setDebugIoTimeoutMsThreshold(int debugIoTimeoutMsThreshold) {
    this.debugIoTimeoutMsThreshold = debugIoTimeoutMsThreshold;
  }

  public boolean isStreamIo() {
    return streamIo;
  }

  public void setStreamIo(boolean streamIo) {
    this.streamIo = streamIo;
  }

  public boolean isEnableLoggerTracer() {
    return enableLoggerTracer;
  }

  public void setEnableLoggerTracer(boolean enableLoggerTracer) {
    this.enableLoggerTracer = enableLoggerTracer;
  }

  public int getTimePullVolumeAccessRulesIntervalMs() {
    return timePullVolumeAccessRulesIntervalMs;
  }

  public void setTimePullVolumeAccessRulesIntervalMs(int timePullVolumeAccessRulesIntervalMs) {
    this.timePullVolumeAccessRulesIntervalMs = timePullVolumeAccessRulesIntervalMs;
  }

  public int getReportDriverInfoIntervalTimeMs() {
    return reportDriverInfoIntervalTimeMs;
  }

  public void setReportDriverInfoIntervalTimeMs(int reportDriverInfoIntervalTimeMs) {
    this.reportDriverInfoIntervalTimeMs = reportDriverInfoIntervalTimeMs;
  }

  public boolean isTraceAllLogs() {
    return traceAllLogs;
  }

  public void setTraceAllLogs(boolean traceAllLogs) {
    this.traceAllLogs = traceAllLogs;
  }

  public String getPerformanceFilePath() {
    return performanceFilePath;
  }

  public void setPerformanceFilePath(String performanceFilePath) {
    this.performanceFilePath = performanceFilePath;
  }

  public int getTrySendIoSemaphoreTimeoutMs() {
    return trySendIoSemaphoreTimeoutMs;
  }

  public void setTrySendIoSemaphoreTimeoutMs(int trySendIoSemaphoreTimeoutMs) {
    this.trySendIoSemaphoreTimeoutMs = trySendIoSemaphoreTimeoutMs;
  }

  public int getTrySemaphoreMaxCountPerTime() {
    return trySemaphoreMaxCountPerTime;
  }

  public void setTrySemaphoreMaxCountPerTime(int trySemaphoreMaxCountPerTime) {
    this.trySemaphoreMaxCountPerTime = trySemaphoreMaxCountPerTime;
  }

  public int getDiscardLogMaxCountPerRequest() {
    return discardLogMaxCountPerRequest;
  }

  public void setDiscardLogMaxCountPerRequest(int discardLogMaxCountPerRequest) {
    this.discardLogMaxCountPerRequest = discardLogMaxCountPerRequest;
  }

  public int getNetworkDelayRecordTimeWindowSec() {
    return networkDelayRecordTimeWindowSec;
  }

  public void setNetworkDelayRecordTimeWindowSec(int networkDelayRecordTimeWindowSec) {
    this.networkDelayRecordTimeWindowSec = networkDelayRecordTimeWindowSec;
  }

  public int getNetworkDelayMeanOutputIntervalSec() {
    return networkDelayMeanOutputIntervalSec;
  }

  public void setNetworkDelayMeanOutputIntervalSec(int networkDelayMeanOutputIntervalSec) {
    this.networkDelayMeanOutputIntervalSec = networkDelayMeanOutputIntervalSec;
  }


  public int getIoLogSequentialCondition() {
    return ioLogSequentialCondition;
  }

  public void setIoLogSequentialCondition(int ioLogSequentialCondition) {
    this.ioLogSequentialCondition = ioLogSequentialCondition;
  }

  @Override
  public String toString() {
    return "CoordinatorConfigSingleton{"
        + "DISCARD_STEP_LEN=" + discardStepLen
        + ", writeIOTimeoutMs=" + writeIoTimeoutMs
        + ", readIOTimeoutMs=" + readIoTimeoutMs
        + ", thriftRequestTimeoutMs=" + thriftRequestTimeoutMs
        + ", thriftConnectTimeoutMs=" + thriftConnectTimeoutMs
        + ", nettyRequestTimeoutMs=" + nettyRequestTimeoutMs
        + ", nettyConnectTimeoutMs=" + nettyConnectTimeoutMs
        + ", commitLogsAfterNoWriteRequestMs=" + commitLogsAfterNoWriteRequestMs
        + ", segmentSize=" + segmentSize
        + ", pageSize=" + pageSize
        + ", maxWriteDataSizePerRequest=" + maxWriteDataSizePerRequest
        + ", maxReadDataSizePerRequest=" + maxReadDataSizePerRequest
        + ", pageWrappedCount=" + pageWrappedCount
        + ", shutdown=" + shutdown
        + ", pageCacheForRead=" + pageCacheForRead
        + ", readCacheForIO=" + readCacheForIo
        + ", connectionPoolSize=" + connectionPoolSize
        + ", resendDelayTimeUnitMs=" + resendDelayTimeUnitMs
        + ", ioDepth=" + ioDepth
        + ", rwLogFlag=" + rwLogFlag
        + ", dataStoragePath='" + dataStoragePath + '\''
        + ", dataStorageSizeGB=" + dataStorageSizeGB
        + ", lowerLimitIops=" + lowerLimitIops
        + ", lowerLimitThroughput=" + lowerLimitThroughput
        + ", largePageSizeForPool=" + largePageSizeForPool
        + ", mediumPageSizeForPool=" + mediumPageSizeForPool
        + ", littlePageSizeForPool=" + littlePageSizeForPool
        + ", cachePoolSize='" + cachePoolSize + '\''
        + ", enableProcessSsd=" + enableProcessSsd
        + ", appName='" + appName + '\''
        + ", healthCheckerRate=" + healthCheckerRate
        + ", localDihPort=" + localDihPort
        + ", maxChannelPendingSize=" + maxChannelPendingSize
        + ", pingHostTimeoutMs=" + pingHostTimeoutMs
        + ", networkConnectionDetectRetryMaxtimes=" + networkConnectionDetectRetryMaxtimes
        + ", debugIoTimeoutMsThreshold=" + debugIoTimeoutMsThreshold
        + ", streamIO=" + streamIo
        + ", enableLoggerTracer=" + enableLoggerTracer
        + ", convertPosType='" + convertPosType + '\''
        + ", maxNetworkFrameSize=" + maxNetworkFrameSize
        + ", timePullVolumeAccessRulesIntervalMs=" + timePullVolumeAccessRulesIntervalMs
        + ", reportDriverInfoIntervalTimeMs=" + reportDriverInfoIntervalTimeMs
        + ", traceAllLogs=" + traceAllLogs
        + ", trySendIOSemaphoreTimeoutMs=" + trySendIoSemaphoreTimeoutMs
        + ", trySemaphoreMaxCountPerTime=" + trySemaphoreMaxCountPerTime
        + ", discardLogMaxCountPerRequest=" + discardLogMaxCountPerRequest
        + ", networkConnectionDetectServerListeningPort="
        + networkConnectionDetectServerListeningPort

        + ", performanceFilePath='" + performanceFilePath + '\''
        + ", networkDelayRecordTimeWindowSec=" + networkDelayRecordTimeWindowSec
        + ", networkDelayMeanOutputIntervalSec=" + networkDelayMeanOutputIntervalSec
        + '}';
  }

  public int getMaxBufLengthForNettyAllocateAdapter() {
    return maxBufLengthForNettyAllocateAdapter;
  }

  public void setMaxBufLengthForNettyAllocateAdapter(int maxBufLengthForNettyAllocateAdapter) {
    this.maxBufLengthForNettyAllocateAdapter = maxBufLengthForNettyAllocateAdapter;
  }

  public IoEventThreadsMode getIoEventGroupThreadsMode() {
    return ioEventGroupThreadsMode;
  }

  public void setIoEventGroupThreadsMode(IoEventThreadsMode ioEventGroupThreadsMode) {
    this.ioEventGroupThreadsMode = ioEventGroupThreadsMode;
  }

  public float getIoEventGroupThreadsParameter() {
    return ioEventGroupThreadsParameter;
  }

  public void setIoEventGroupThreadsParameter(float ioEventGroupThreadsParameter) {
    this.ioEventGroupThreadsParameter = ioEventGroupThreadsParameter;
  }

  public IoEventThreadsMode getIoEventHandleThreadsMode() {
    return ioEventHandleThreadsMode;
  }

  public void setIoEventHandleThreadsMode(IoEventThreadsMode ioEventHandleThreadsMode) {
    this.ioEventHandleThreadsMode = ioEventHandleThreadsMode;
  }

  public float getIoEventHandleThreadsParameter() {
    return ioEventHandleThreadsParameter;
  }

  public void setIoEventHandleThreadsParameter(float ioEventHandleThreadsParameter) {
    this.ioEventHandleThreadsParameter = ioEventHandleThreadsParameter;
  }

  public IoEventThreadsMode getClientIoEventGroupThreadsMode() {
    return clientIoEventGroupThreadsMode;
  }

  public void setClientIoEventGroupThreadsMode(
      IoEventThreadsMode clientIoEventGroupThreadsMode) {
    this.clientIoEventGroupThreadsMode = clientIoEventGroupThreadsMode;
  }

  public float getClientIoEventGroupThreadsParameter() {
    return clientIoEventGroupThreadsParameter;
  }

  public void setClientIoEventGroupThreadsParameter(float clientIoEventGroupThreadsParameter) {
    this.clientIoEventGroupThreadsParameter = clientIoEventGroupThreadsParameter;
  }

  public IoEventThreadsMode getClientIoEventHandleThreadsMode() {
    return clientIoEventHandleThreadsMode;
  }

  public void setClientIoEventHandleThreadsMode(
      IoEventThreadsMode clientIoEventHandleThreadsMode) {
    this.clientIoEventHandleThreadsMode = clientIoEventHandleThreadsMode;
  }

  public float getClientIoEventHandleThreadsParameter() {
    return clientIoEventHandleThreadsParameter;
  }

  public void setClientIoEventHandleThreadsParameter(float clientIoEventHandleThreadsParameter) {
    this.clientIoEventHandleThreadsParameter = clientIoEventHandleThreadsParameter;
  }
}


