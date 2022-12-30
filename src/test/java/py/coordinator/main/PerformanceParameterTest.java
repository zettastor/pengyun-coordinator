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
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import org.apache.log4j.Logger;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.drivercontainer.JvmConfigurationForDriver;
import py.drivercontainer.utils.JavaProcessBuilder;
import py.performance.PerformanceParameter;
import py.performance.PerformanceRecorder;
import py.test.TestBase;

/**
 * xx.
 */
public class PerformanceParameterTest extends TestBase {

  private static Logger logger = Logger.getLogger(PerformanceParameterTest.class);
  private static String tmpPath = "/tmp/performance.cfg";
  private static JvmConfigurationForDriver jvmConfig = new JvmConfigurationForDriver();
  private long volumeId = RequestIdBuilder.get();

  @Override
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void writeAndReadFile() {
    PerformanceRecorder performanceRecorder = new PerformanceRecorder();
    performanceRecorder.setReadLatencyNs(200);
    performanceRecorder.setWriteLatencyNs(300);
    performanceRecorder.setReadCounter(50);
    performanceRecorder.setReadDataSizeBytes(200000);
    performanceRecorder.setWriteDataSizeBytes(300000);
    performanceRecorder.setRecordTimeIntervalMs(500);
    performanceRecorder.setWriteCounter(60);

    int i = 0;
    while (i++ < 10) {
      performanceRecorder.addReadCounter(1);
      performanceRecorder.addWriteCounter(1);

      PerformanceParameter performanceParameter = performanceRecorder.getPerformanceParameter();
      assertTrue(writeToFile(tmpPath, performanceParameter));

      PerformanceParameter returnValue = readFromFile(tmpPath);
      assertTrue(returnValue != null);

      assertTrue(returnValue.getReadLatencyNs() == performanceParameter.getReadLatencyNs());
      assertTrue(returnValue.getReadCounter() == performanceParameter.getReadCounter());
      assertTrue(returnValue.getReadDataSizeBytes() == performanceParameter.getReadDataSizeBytes());
      assertTrue(
          returnValue.getWriteDataSizeBytes() == performanceParameter.getWriteDataSizeBytes());
      assertTrue(
          returnValue.getRecordTimeIntervalMs() == performanceParameter.getRecordTimeIntervalMs());
      assertTrue(returnValue.getWriteCounter() == performanceParameter.getWriteCounter());
      assertTrue(returnValue.getWriteLatencyNs() == performanceParameter.getWriteLatencyNs());
    }
  }

  @Test
  public void syncReadWriteFile() throws IOException, InterruptedException {
    File file = new File(tmpPath);
    if (file.exists()) {
      file.delete();
    }

    if (!file.createNewFile()) {
      fail("can not create file : " + tmpPath);
    }
    PerformanceRecorder performanceRecorder = new PerformanceRecorder();
    final int lockTime = 3000;

    performanceRecorder.setReadLatencyNs(200);
    performanceRecorder.setWriteLatencyNs(300);
    performanceRecorder.setReadCounter(0);
    performanceRecorder.setReadDataSizeBytes(200000);
    performanceRecorder.setWriteDataSizeBytes(300000);
    performanceRecorder.setRecordTimeIntervalMs(500);
    performanceRecorder.setWriteCounter(0);

    // start a process to lock the file
    jvmConfig.setMainClass("py.coordinator.main.TestBuildProcess");
    JavaProcessBuilder javaProcessBuilder = new JavaProcessBuilder(jvmConfig);
    javaProcessBuilder.addArgument(tmpPath);
    javaProcessBuilder.addArgument(Integer.toString(lockTime));
    javaProcessBuilder.setWorkingDirectory("/tmp");
    Process process = javaProcessBuilder.startProcess();
    if (process == null) {
      fail("fail to create process ");
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;
    while ((line = br.readLine()) != null) {
      System.out.println("process: " + line);
      if (line.contains("yes lock")) {
        break;
      }
    }

    RandomAccessFile randomAccessFile = null;
    FileLock fileLock = null;
    FileChannel fileChannel = null;
    try {
      randomAccessFile = new RandomAccessFile(tmpPath, "rw");
      fileChannel = randomAccessFile.getChannel();
      int times = 0;
      while (true) {
        fileLock = fileChannel.tryLock();
        if (fileLock != null) {
          break;
        } else {
          times++;
        }

        Thread.sleep(1000);
      }

      assertTrue(times >= 0);
      logger.info("lock times: " + times);

      PerformanceParameter performanceParameter = performanceRecorder.getPerformanceParameter();

      if (fileLock != null && fileLock.isValid()) {
        byte[] context = performanceParameter.toJsonString().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(context, 0, context.length);
        fileChannel.write(byteBuffer);
      }

      Thread.sleep(1000);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
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
      } catch (IOException e) {
        logger.error("caught exception", e);
      }
    }
  }


  /**
   * xx.
   */
  public boolean writeToFile(String filePath, PerformanceParameter performanceParameter) {

    File file = new File(filePath);
    if (!file.exists()) {
      try {
        if (!file.createNewFile()) {
          logger.error("can not create file:" + filePath);
          return false;
        }
      } catch (IOException e) {
        return false;
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
        logger.warn("can not get the file lock for file: " + filePath);
      } else {
        byte[] context = performanceParameter.toJsonString().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(context, 0, context.length);
        fileChannel.write(byteBuffer);
      }
    } catch (Exception e) {
      return false;
    } finally {
      try {
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
      } catch (Exception e) {
        logger.error("caught exception", e);
      }

    }
    return true;
  }


  /**
   * xx.
   */
  public PerformanceParameter readFromFile(String filePath) {
    File file = new File(filePath);
    if (!file.exists()) {
      logger.warn("can not find the file " + filePath + " for getting the performance parameter");
      return null;
    }

    // compare the modify time and only the time has been been updated, then get the new information
    PerformanceParameter performanceParameter;
    FileChannel fileChannel = null;
    FileLock fileLock = null;
    RandomAccessFile randomAccessFile = null;

    try {
      randomAccessFile = new RandomAccessFile(filePath, "rw");
      fileChannel = randomAccessFile.getChannel();
      int times = 10;
      while (fileLock == null && times-- > 0) {
        fileLock = fileChannel.tryLock();
        if (fileLock == null) {
          Thread.sleep(50);
        }
      }

      byte[] context = new byte[(int) randomAccessFile.length()];
      randomAccessFile.readFully(context);

      performanceParameter = PerformanceParameter
          .fromJson(new String(context, Charset.defaultCharset()));
    } catch (Exception e) {
      logger.warn("can not read file:" + filePath);
      return null;
    } finally {

      try {
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
      } catch (IOException e) {
        logger.warn("close the fileLock failure");
      }

    }

    return performanceParameter;
  }
}
