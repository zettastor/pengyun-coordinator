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

package py.coordinator.main;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import py.common.PyService;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.performance.PerformanceRecorder;
import py.querylog.eventdatautil.EventDataWorker;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;

/**
 * cover all paths that the coordinator should deal with.
 */
public class OutPutTest extends TestBase {

  Random random = new Random();
  long volumeIdThread1 = 1234566;
  long volumeIdThread2 = 1234567;

  @Test
  public void testThread() {
    Thread thread1 = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          processAndOutput(volumeIdThread1, "1");
          try {
            Thread.sleep(random.nextInt(200) + 200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

        }
      }
    });

    Thread thread2 = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          processAndOutput(volumeIdThread2, "2");
          try {
            Thread.sleep(random.nextInt(200) + 300);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

        }
      }
    });

    thread1.start();
    thread2.start();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


  }


  /**
   * xx.
   */
  public void processAndOutput(long volumeId, String threadName) {
    PerformanceRecorder performanceParameter = new PerformanceRecorder();
    performanceParameter.setReadCounter(50);
    performanceParameter.setWriteCounter(60);
    performanceParameter.setWriteDataSizeBytes(300000);
    performanceParameter.setReadDataSizeBytes(200000);
    performanceParameter.setReadLatencyNs(200);
    performanceParameter.setWriteLatencyNs(300);
    performanceParameter.setRecordTimeIntervalMs(500);

    try {
      Map<String, Long> counters = new HashMap<>();
      // write iops
      long writeIops;
      if (performanceParameter.getWriteCounter() == 0) {
        writeIops = 0;
      } else {
        writeIops =
            performanceParameter.getWriteCounter() * 1000 / performanceParameter
                .getRecordTimeIntervalMs();
      }
      counters.put(CounterName.VOLUME_WRITE_IOPS.name(), writeIops);

      // read iops
      long readIops;
      if (performanceParameter.getReadCounter() == 0) {
        readIops = 0;
      } else {
        readIops =
            performanceParameter.getReadCounter() * 1000 / performanceParameter
                .getRecordTimeIntervalMs();
      }
      counters.put(CounterName.VOLUME_READ_IOPS.name(), readIops);

      // write throughput
      long writeThroughput;
      if (performanceParameter.getWriteDataSizeBytes() == 0) {
        writeThroughput = 0;
      } else {
        writeThroughput = performanceParameter.getWriteDataSizeBytes() * 1000 / performanceParameter
            .getRecordTimeIntervalMs();

        writeThroughput /= 1024; // KB
      }
      counters.put(CounterName.VOLUME_WRITE_THROUGHPUT.name(), writeThroughput);

      // read throughput
      long readThroughput;
      if (performanceParameter.getReadDataSizeBytes() == 0) {
        readThroughput = 0;
      } else {
        readThroughput = performanceParameter.getReadDataSizeBytes() * 1000 / performanceParameter
            .getRecordTimeIntervalMs();
        readThroughput /= 1024; // KB
      }
      counters.put(CounterName.VOLUME_READ_THROUGHPUT.name(), readThroughput);

      // write latency
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

      // read latency
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

      // io block size
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

      Map<String, String> userDefineParaMs = new HashMap<>();
      userDefineParaMs.put(UserDefineName.VolumeID.name(), String.valueOf(volumeId));
      userDefineParaMs.put(UserDefineName.StoragePoolID.name(), "3456");
      userDefineParaMs.put(UserDefineName.VolumeName.name(), "volumeTest" + threadName);
      userDefineParaMs.put(UserDefineName.StoragePoolName.name(), "poolTest");

      EdRootpathSingleton edRootpathSingleton = EdRootpathSingleton.getInstance();
      edRootpathSingleton.setRootPath("/tmp/testing");

      EventDataWorker eventDataWorker = new EventDataWorker(PyService.COORDINATOR,
          userDefineParaMs);
      String result = eventDataWorker.work(OperationName.Volume.name(), counters);
      boolean findError = false;
      if (findCount(result, "EOQ") != 1) {
        logger.error("when outPut ,get the error: ");
        findError = true;
      }

      assertTrue(!findError);

    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }


  /**
   * xx.
   */
  public int findCount(String src, String des) {
    int index = 0;
    int count = 0;
    while ((index = src.indexOf(des, index)) != -1) {
      count++;
      index = index + des.length();
    }
    return count;
  }

}
