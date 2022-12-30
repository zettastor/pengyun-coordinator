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

package py.coordinator.worker;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.VolumeInfoHolderManager;
import py.coordinator.utils.NetworkDelayRecorder;
import py.coordinator.volumeinfo.SpaceSavingVolumeMetadata;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.performance.PerformanceManager;
import py.performance.PerformanceParameter;
import py.performance.PerformanceRecorder;
import py.periodic.Worker;
import py.querylog.eventdatautil.EventDataWorker;
import py.thrift.share.StoragePoolThrift;


public class OutputPerformanceWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(OutputPerformanceWorker.class);
  private static final AtomicLong runCount = new AtomicLong(0);
  private final int eoqNumber = 1;
  private PerformanceManager performanceManager;
  private String filePath;
  private long lastRecordTime = 0;
  private VolumeInfoHolderManager volumeInfoHolderManager;
  private String myHostname;
  private NetworkDelayRecorder networkDelayRecorder;
  private int networkDelayMeanOutputIntervalSec;



  public OutputPerformanceWorker(VolumeInfoHolderManager volumeInfoHolderManager,
      NetworkDelayRecorder networkDelayRecorder) {
    this.volumeInfoHolderManager = volumeInfoHolderManager;
    this.networkDelayRecorder = networkDelayRecorder;
    this.performanceManager = PerformanceManager.getInstance();
    Path path = Paths.get(CoordinatorConfigSingleton.getInstance().getPerformanceFilePath());
    if (!Files.exists(path)) {
      try {
        Files.createDirectories(path);
      } catch (Exception e) {
        logger.error("can not create directories:{}", path, e);
        throw new RuntimeException();
      }
    }

  }



  public static int findCount(String src, String des) {
    int index = 0;
    int count = 0;
    while ((index = src.indexOf(des, index)) != -1) {
      count++;
      index = index + des.length();
    }
    return count;
  }

  public String getMyHostname() {
    return myHostname;
  }

  public void setMyHostname(String myHostname) {
    this.myHostname = myHostname;
  }

  public int getNetworkDelayMeanOutputIntervalSec() {
    return networkDelayMeanOutputIntervalSec;
  }

  public void setNetworkDelayMeanOutputIntervalSec(int networkDelayMeanOutputIntervalSec) {
    this.networkDelayMeanOutputIntervalSec = networkDelayMeanOutputIntervalSec;
  }

  @Override
  public void doWork() throws Exception {
    runCount.incrementAndGet();

    if (lastRecordTime == 0) {
      lastRecordTime = System.currentTimeMillis();
      return;
    }

    if (runCount.get() % networkDelayMeanOutputIntervalSec == 0) {

      networkDelayRecorder.outputAllMeanDelay(myHostname);
    }


    long currTime = System.currentTimeMillis();
    long diffTime = currTime - lastRecordTime;
    if (diffTime <= 0) {
      logger.debug("the diff is zero, the last time :{} lastRecordTime is :{}", currTime,
          lastRecordTime);
      return;
    }
    lastRecordTime = currTime;



    Map<Long, PerformanceRecorder> map = performanceManager.getPerformanceManagerMap();
    for (Map.Entry<Long, PerformanceRecorder> recorder : map.entrySet()) {

      PerformanceRecorder performanceRecorder = recorder.getValue();
      performanceRecorder.setRecordTimeIntervalMs(diffTime);
      PerformanceParameter performanceParameter = performanceRecorder.getPerformanceParameter();
      processAndOutput(recorder.getKey(), performanceParameter);

      File file = new File(filePath);
      if (!file.exists()) {
        if (!file.createNewFile()) {
          logger.error("can not create file:{}", filePath);
          return;
        }
      }

      RandomAccessFile randomAccessFile = null;
      FileLock fileLock = null;
      FileChannel fileChannel = null;
      try {
        randomAccessFile = new RandomAccessFile(filePath, "rw");
        fileChannel = randomAccessFile.getChannel();
        fileLock = fileChannel.tryLock();

        int count = 10;
        while (fileLock == null && count-- > 0) {
          fileLock = fileChannel.tryLock();
          if (fileLock == null) {
            Thread.sleep(50);
          }
        }

        if (fileLock == null) {
          logger.warn("can not get the file lock for file: {}", filePath);
        } else {
          byte[] context = performanceParameter.toJsonString().getBytes();
          ByteBuffer byteBuffer = ByteBuffer.wrap(context, 0, context.length);
          fileChannel.write(byteBuffer);
        }

      } finally {
        if (fileLock != null) {
          fileLock.release();
          fileLock.close();
        }

        if (fileChannel != null) {
          fileChannel.close();
        }
        if (randomAccessFile != null) {
          randomAccessFile.close();
        }
      }
    }
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  private void processAndOutput(long volumeId, PerformanceParameter performanceParameter) {
    try {
      Map<String, Long> counters = new HashMap<>();

      long writeIops;
      if (performanceParameter.getWriteCounter() == 0) {
        writeIops = 0;
      } else {
        writeIops =
            performanceParameter.getWriteCounter() * 1000 / performanceParameter
                .getRecordTimeIntervalMs();
      }
      counters.put(CounterName.VOLUME_WRITE_IOPS.name(), writeIops);


      long readIops;
      if (performanceParameter.getReadCounter() == 0) {
        readIops = 0;
      } else {
        readIops =
            performanceParameter.getReadCounter() * 1000 / performanceParameter
                .getRecordTimeIntervalMs();
      }
      counters.put(CounterName.VOLUME_READ_IOPS.name(), readIops);


      long writeThroughput;
      if (performanceParameter.getWriteDataSizeBytes() == 0) {
        writeThroughput = 0;
      } else {
        writeThroughput = performanceParameter.getWriteDataSizeBytes() * 1000 / performanceParameter
            .getRecordTimeIntervalMs();

        writeThroughput /= 1024;
      }
      counters.put(CounterName.VOLUME_WRITE_THROUGHPUT.name(), writeThroughput);


      long readThroughput;
      if (performanceParameter.getReadDataSizeBytes() == 0) {
        readThroughput = 0;
      } else {
        readThroughput = performanceParameter.getReadDataSizeBytes() * 1000 / performanceParameter
            .getRecordTimeIntervalMs();
        readThroughput /= 1024;
      }
      counters.put(CounterName.VOLUME_READ_THROUGHPUT.name(), readThroughput);


      long writeLatency;
      if (performanceParameter.getWriteCounter() == 0) {
        writeLatency = 0;
      } else {
        if (performanceParameter.getWriteLatencyNs() == 0) {
          writeLatency = 0;
        } else {
          writeLatency = (performanceParameter.getWriteLatencyNs() / 1000) / performanceParameter
              .getWriteCounter();
        }
      }
      counters.put(CounterName.VOLUME_WRITE_LATENCY.name(), writeLatency);


      long readLatency;
      if (performanceParameter.getReadCounter() == 0) {
        readLatency = 0;
      } else {
        if (performanceParameter.getReadLatencyNs() == 0) {
          readLatency = 0;
        } else {
          readLatency = (performanceParameter.getReadLatencyNs() / 1000) / performanceParameter
              .getReadCounter();
        }
      }
      counters.put(CounterName.VOLUME_READ_LATENCY.name(), readLatency);


      long ioBlockSize;
      long allIoCount =
          performanceParameter.getReadCounter() + performanceParameter.getWriteCounter();
      if (allIoCount == 0) {
        ioBlockSize = 0;
      } else {
        long allIoThroughput =
            performanceParameter.getReadDataSizeBytes() + performanceParameter
                .getWriteDataSizeBytes();
        if (allIoThroughput == 0) {
          ioBlockSize = 0;
        } else {
          ioBlockSize = allIoThroughput / allIoCount;
        }
      }
      counters.put(CounterName.VOLUME_IO_BLOCK_SIZE.name(), ioBlockSize);

      SpaceSavingVolumeMetadata volumeMetadata = volumeInfoHolderManager
          .getVolumeInfoHolder(volumeId).getVolumeMetadata();
      StoragePoolThrift storagePoolThrift = volumeInfoHolderManager.getVolumeInfoHolder(volumeId)
          .getStoragePoolThrift();

      Map<String, String> userDefineParaMs = new HashMap<>();
      userDefineParaMs.put(UserDefineName.VolumeID.name(), String.valueOf(volumeId));
      userDefineParaMs
          .put(UserDefineName.StoragePoolID.name(), String.valueOf(storagePoolThrift.getPoolId()));
      userDefineParaMs.put(UserDefineName.VolumeName.name(), volumeMetadata.getName());
      userDefineParaMs.put(UserDefineName.StoragePoolName.name(), storagePoolThrift.getPoolName());

      logger.info("get the end value ,volume id is :{},userDefineParaMs is :{}", volumeId,
          userDefineParaMs);

      EventDataWorker eventDataWorker = new EventDataWorker(PyService.COORDINATOR,
          userDefineParaMs);
      String getResult = eventDataWorker.work(OperationName.Volume.name(), counters);
      if (findCount(getResult, "EOQ") != eoqNumber) {
        logger.error("when output, the write information:{} is error", getResult);
      }

    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }
}