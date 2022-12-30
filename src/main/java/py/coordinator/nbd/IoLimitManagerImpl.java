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

package py.coordinator.nbd;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.IoLimitManager;
import py.io.qos.IoLimitationEntry;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.ResetSlowLevelRequest;


public class IoLimitManagerImpl implements IoLimitManager {

  private static final Logger logger = LoggerFactory.getLogger(IoLimitManagerImpl.class);

  private final long throughputAccuracy = 1000;

  private final int puttingRate = 200;

  private final int checkingRate = 1000;

  private final IntValue lowerIops;
  private final IntValue upperIops;
  private final IntValue lowerIopsT;
  private final IntValue upperIopsT;
  private final int setLimitForLastTime = 1;
  private IoLimitationEntry ioLimitationEntry;
  private int slowLevel = 0;
  private int slowCountDown = 0;
  private AtomicInteger countIO;
  private AtomicInteger countVirtualIoForThroughput;
  private boolean open = false;
  private boolean lowerIopsWorking = false;
  private boolean upperIopsWorking = false;
  private boolean lowerThroughputWorking = false;
  private boolean upperThroughputWorking = false;
  private LinkedBlockingQueue<String> tokenBucketForIops;
  private LinkedBlockingQueue<String> tokenBucketForThroughput;
  private InstanceStore instanceStore;
  private CoordinatorClientFactory coordinatorClientFactory;
  private LaunchDriverParameters launchDriverParameters;

  private PeriodicWorkExecutorImpl upperIopsLimitExecutor;
  private PeriodicWorkExecutorImpl upperThroughputLimitExecutor;
  private PeriodicWorkExecutorImpl lowerIopsLimitExecutor;
  private PeriodicWorkExecutorImpl lowerThroughputLimitExecutor;



  public IoLimitManagerImpl(LaunchDriverParameters launchDriverParameters) {
    this.launchDriverParameters = launchDriverParameters;
    this.countIO = new AtomicInteger(0);
    this.countVirtualIoForThroughput = new AtomicInteger(0);
    lowerIops = new IntValue(0);
    upperIops = new IntValue(0);
    lowerIopsT = new IntValue(0);
    upperIopsT = new IntValue(0);
  }

  @Override
  public void updateLimitationsAndOpen(IoLimitationEntry ioLimitationEntry)
      throws UnableToStartException {
    this.ioLimitationEntry = ioLimitationEntry;
    logger.info("limitations updated, io ioLimitationEntry : {}", this.ioLimitationEntry);
    updateLimitValue();
    if (!lowerIopsWorking) {
      startIopsGuarantee();
    }
    if (!upperIopsWorking) {
      startIopsLimitation();
    }
    if (!lowerThroughputWorking) {
      startThroughputGuarantee();
    }
    if (!upperThroughputWorking) {
      startThroughputLimitation();
    }
    open = true;
  }

  private void updateLimitValue() {

    if (ioLimitationEntry.enableUpperThroughput()) {
      upperIopsT
          .setValue((int) (ioLimitationEntry.getUpperLimitedThroughput() / throughputAccuracy));
    } else {
      upperIopsT.setValue(0);
      if (upperThroughputWorking) {
        upperThroughputLimitExecutor.stopNow();
        upperThroughputWorking = false;
      }
    }

    if (ioLimitationEntry.enableLowerThroughput()) {
      lowerIopsT
          .setValue((int) (ioLimitationEntry.getLowerLimitedThroughput() / throughputAccuracy));
    } else {
      lowerIopsT.setValue(0);
      if (lowerThroughputWorking) {
        lowerThroughputLimitExecutor.stopNow();
        lowerThroughputWorking = false;
      }
    }

    if (ioLimitationEntry.enableLowerIops()) {
      lowerIops.setValue(ioLimitationEntry.getLowerLimitedIops());
    } else {
      lowerIops.setValue(0);
      if (lowerIopsWorking) {
        lowerIopsLimitExecutor.stopNow();
        lowerIopsWorking = false;
      }
    }

    if (ioLimitationEntry.enableUpperIops()) {
      upperIops.setValue(ioLimitationEntry.getUpperLimitedIops());
    } else {
      upperIops.setValue(0);
      if (upperIopsWorking) {
        upperIopsLimitExecutor.stopNow();
        upperIopsWorking = false;
      }
    }
  }

  private boolean enableLowerLimit() {
    if (ioLimitationEntry == null) {
      return false;
    }
    if (ioLimitationEntry.enableLowerThroughput() || ioLimitationEntry.enableLowerIops()) {
      return true;
    }
    return false;
  }

  /**
   * every time an IO comes, try to get a token from the token bucket. If the IOs are coming too
   * fast, block here to slow them down
   */
  public void tryGettingAnIo() {
    countIO.addAndGet(1);
    if (slowLevel > 0 && slowCountDown > 5 && !enableLowerLimit()) {
      try {
        logger.debug("now to slow {} mills", slowLevel);
        Thread.sleep(slowLevel);
        slowCountDown = 0;
      } catch (Exception e) {
        logger.warn("cannot slow down");
      }
    }
    slowCountDown++;

    if (ioLimitationEntry == null || !upperIopsWorking) {
      logger.debug("iops limitation disabled just pass ioLimitationEntry :{} upperIopsWorking :{}",
          ioLimitationEntry, upperIopsWorking);
      return;
    } else if (ioLimitationEntry.enableUpperIops()) {

      while (tokenBucketForIops == null) {
        logger.warn("io limit manager is not ready yet, wait 3 seconds and try again");
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          logger.error("something went wrong, io limitation is not working");
          return;
        }
      }

      logger.info("now to get token. bucket size: {}, and the upperIopsWorking :{}, open :{}",
          tokenBucketForIops.size(), upperIopsWorking, open);
      long time1 = System.currentTimeMillis();
      try {
        while (upperIopsWorking && open) {
          logger.info("i am going to poll token");
          if (tokenBucketForIops.poll(2, TimeUnit.SECONDS) != null) {
            break;
          } else {
            logger.warn("can't get token !! keep trying");
          }
        }
      } catch (InterruptedException e) {
        logger.error("cannot take a token from bucket, Iops limitation is not working");
      }
      logger.debug("got token: bucket size: {}, time: {}", tokenBucketForIops.size(),
          System.currentTimeMillis() - time1);
    }
  }


  public void tryThroughput(long ioSize) {
    int numVirtualIO = (int) (ioSize / throughputAccuracy);
    countVirtualIoForThroughput.addAndGet(numVirtualIO);

    if (ioLimitationEntry == null || !upperThroughputWorking) {
      logger.debug("throughput limitation disabled just pass");
      return;
    } else if (ioLimitationEntry.enableUpperThroughput()) {

      while (tokenBucketForThroughput == null) {
        logger.warn("io limit manager is not ready yet, wait 3 seconds and try again");
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          logger.error("something went wrong, io limitation is not working");
          return;
        }
      }

      try {
        logger.debug("want to take {} tokens size for now {}", numVirtualIO,
            tokenBucketForThroughput.size());
        for (int i = 0; i < numVirtualIO; i++) {
          while (upperThroughputWorking && open) {
            if (tokenBucketForThroughput.poll(2, TimeUnit.SECONDS) != null) {
              break;
            } else {
              logger.warn("can't get token !! keep trying");
            }
          }
        }
        logger.debug("token got , pass");
      } catch (InterruptedException e) {
        logger.error("cannot take tolen from bucked, throughput limitation is not working");
      }
    }
  }


  public void startThroughputLimitation() throws UnableToStartException {

    if (upperIopsT.getValue() == 0 || !ioLimitationEntry.enableUpperThroughput()) {
      logger.debug("upper throughput not enabled");
      return;
    }
    tokenBucketForThroughput = new LinkedBlockingQueue<String>();
    upperThroughputLimitExecutor = new PeriodicWorkExecutorImpl();
    ExecutionOptionsReader putTokenExecutionOptionReader = new ExecutionOptionsReader(1, 1,
        puttingRate, null);
    IoControllerFactory ioControllerFactory = new IoControllerFactory(upperIopsT, true,
        puttingRate);
    ioControllerFactory.setTokenBucket(tokenBucketForThroughput);

    upperThroughputLimitExecutor.setExecutionOptionsReader(putTokenExecutionOptionReader);
    upperThroughputLimitExecutor.setWorkerFactory(ioControllerFactory);

    upperThroughputLimitExecutor.start();
    upperThroughputWorking = true;
  }


  public void startIopsLimitation() throws UnableToStartException {
    if (upperIops.getValue() == 0 || !ioLimitationEntry.enableUpperIops()) {
      logger.debug("upper iops not enabled");
      return;
    }
    logger.debug("upperIopsWorking ok-----");
    tokenBucketForIops = new LinkedBlockingQueue<String>();
    upperIopsLimitExecutor = new PeriodicWorkExecutorImpl();
    ExecutionOptionsReader putTokenExecutionOptionReader = new ExecutionOptionsReader(1, 1,
        puttingRate, null);
    IoControllerFactory iopsControllerFactory = new IoControllerFactory(upperIops, true,
        puttingRate);
    iopsControllerFactory.setTokenBucket(tokenBucketForIops);

    upperIopsLimitExecutor.setExecutionOptionsReader(putTokenExecutionOptionReader);
    upperIopsLimitExecutor.setWorkerFactory(iopsControllerFactory);

    upperIopsLimitExecutor.start();
    upperIopsWorking = true;
    logger.debug("upperIopsWorking ok");
  }



  public void startIopsGuarantee() throws UnableToStartException {
    if (lowerIops.getValue() == 0 || !ioLimitationEntry.enableLowerIops()) {
      logger.debug("lower iops not enabled");
      return;
    }
    lowerIopsLimitExecutor = new PeriodicWorkExecutorImpl();
    final ExecutionOptionsReader lowerIopsLimitOptionReader = new ExecutionOptionsReader(1, 1,
        checkingRate, null);

    IoControllerFactory ioControllerFactory = new IoControllerFactory(lowerIops, false,
        checkingRate);
    ioControllerFactory.setCoordinatorClientFactory(coordinatorClientFactory);
    ioControllerFactory.setCountIO(countIO);
    ioControllerFactory.setInstanceStore(instanceStore);
    ioControllerFactory.setVolumeId(launchDriverParameters.getVolumeId());

    lowerIopsLimitExecutor.setExecutionOptionsReader(lowerIopsLimitOptionReader);
    lowerIopsLimitExecutor.setWorkerFactory(ioControllerFactory);

    lowerIopsLimitExecutor.start();
    lowerIopsWorking = true;
  }



  public void startThroughputGuarantee() throws UnableToStartException {
    if (lowerIopsT.getValue() == 0 || !ioLimitationEntry.enableLowerThroughput()) {
      logger.debug("lower throughput not enabled");
      return;
    }

    lowerThroughputLimitExecutor = new PeriodicWorkExecutorImpl();
    final ExecutionOptionsReader lowerThroughputLimitOptionReader = new ExecutionOptionsReader(1, 1,
        checkingRate, null);

    IoControllerFactory ioControllerFactory = new IoControllerFactory(lowerIopsT, false,
        checkingRate);
    ioControllerFactory.setCoordinatorClientFactory(coordinatorClientFactory);
    ioControllerFactory.setCountIO(countVirtualIoForThroughput);
    ioControllerFactory.setInstanceStore(instanceStore);
    ioControllerFactory.setVolumeId(launchDriverParameters.getVolumeId());

    lowerThroughputLimitExecutor.setExecutionOptionsReader(lowerThroughputLimitOptionReader);
    lowerThroughputLimitExecutor.setWorkerFactory(ioControllerFactory);

    lowerThroughputLimitExecutor.start();
    lowerThroughputWorking = true;
  }

  @Override
  public void slowDownExceptFor(long volumeId, int level) {
    if (ioLimitationEntry != null && (ioLimitationEntry.enableLowerIops() || ioLimitationEntry
        .enableLowerThroughput())) {
      return;
    }
    long myVolumeId = launchDriverParameters.getVolumeId();
    if (myVolumeId != volumeId) {
      slowLevel = slowLevel + level;
      logger.debug("slow down level {}", slowLevel);
    }
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  public void setOpen(boolean open) {
    this.open = open;
  }

  @Override
  public void close() {
    logger.warn("try to close the limit manager");
    this.open = false;
    if (tokenBucketForIops != null && tokenBucketForIops.isEmpty()) {
      for (int i = 0; i < setLimitForLastTime; i++) {
        try {
          tokenBucketForIops.put("token");
        } catch (InterruptedException e) {
          logger.warn("when close the io limit, set the end token error");
        }
      }
    }

    stopPutting();
    stopGuaranteeing();
    logger.warn("closed the limit manager");
  }



  public void stopPutting() {
    if (upperIopsLimitExecutor != null) {
      upperIopsLimitExecutor.stopNow();
      upperIopsWorking = false;
    }
    if (upperThroughputLimitExecutor != null) {
      upperThroughputLimitExecutor.stopNow();
      upperThroughputWorking = false;
    }
  }



  public void stopGuaranteeing() {
    if (lowerIopsLimitExecutor != null) {
      lowerIopsLimitExecutor.stopNow();
      lowerIopsWorking = false;
    }
    if (lowerThroughputLimitExecutor != null) {
      lowerThroughputLimitExecutor.stopNow();
      lowerThroughputWorking = false;
    }
    if (lowerIopsLimitExecutor == null && lowerThroughputLimitExecutor == null) {
      return;
    }
    for (Instance coordinatorInstance : instanceStore.getAll(PyService.COORDINATOR.getServiceName(),
        InstanceStatus.HEALTHY)) {
      int retryTimes = 3;
      while (retryTimes > 0) {
        try {
          Coordinator.Iface cclient = coordinatorClientFactory
              .build(coordinatorInstance.getEndPoint())
              .getClient();
          ResetSlowLevelRequest request = new ResetSlowLevelRequest();
          request.setRequestId(RequestIdBuilder.get());
          request.setVolumeId(launchDriverParameters.getVolumeId());
          cclient.resetSlowLevel(request);
          break;
        } catch (Exception e) {
          logger.warn("exception catch", e);
          retryTimes--;
        }
      }
      if (retryTimes == 0) {
        logger.error("cannot reset slow level for", coordinatorInstance.getEndPoint());
      }
    }
  }

  @Override
  public void resetSlowLevel(long volumeId) {
    if (launchDriverParameters.getVolumeId() != volumeId) {
      slowLevel = 0;
    }
  }

  @Override
  public IoLimitationEntry getIoLimitationEntry() {
    return ioLimitationEntry;
  }

  public static class IntValue {

    Integer value;

    public IntValue(Integer value) {
      super();
      this.value = value;
    }

    public Integer getValue() {
      return value;
    }

    public void setValue(Integer value) {
      this.value = value;
    }

  }
}
