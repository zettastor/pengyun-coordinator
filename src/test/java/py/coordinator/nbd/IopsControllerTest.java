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

import static org.junit.Assert.fail;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import py.coordinator.nbd.IoLimitManagerImpl.IntValue;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.test.TestBase;

/**
 * xx.
 */
public class IopsControllerTest extends TestBase {


  private BlockingQueue<String> tokenBucket;

  private int limitedIops = 200;

  private int putRate = 200;

  private PeriodicWorkExecutorImpl putTokenExcuter;

  private int takeCount;


  /**
   * xx.
   */
  @Before
  public void setUp() throws Exception {
    tokenBucket = new ArrayBlockingQueue<String>(limitedIops);
    ExecutionOptionsReader putTokenExecutionOptionReader = new ExecutionOptionsReader(1, 1, putRate,
        null);
    IoControllerFactory iopsControllerFactory = new IoControllerFactory(new IntValue(limitedIops),
        true, putRate);
    iopsControllerFactory.setTokenBucket(tokenBucket);
    putTokenExcuter = new PeriodicWorkExecutorImpl();
    putTokenExcuter.setExecutionOptionsReader(putTokenExecutionOptionReader);
    putTokenExcuter.setWorkerFactory(iopsControllerFactory);
    takeCount = 0;
  }

  public void take() throws InterruptedException {
    tokenBucket.take();
    takeCount++;
  }

  @Test
  public void test() {
    try {
      putTokenExcuter.start();
    } catch (UnableToStartException e) {
      logger.error("caught exception", e);
    }
    long timeBefore = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      try {
        take();
      } catch (InterruptedException e) {
        fail("interrupted while waiting");
      }
    }
    long timeAfter = System.currentTimeMillis();
    int time = (int) (timeAfter - timeBefore);
    int expectedTime = 1000 / limitedIops * 1000;
    logger.warn("the time is :{}, the expectedTime is :{}", time, expectedTime);
    Assert.assertTrue(Math.abs(time - expectedTime) < 1000);
  }

}
