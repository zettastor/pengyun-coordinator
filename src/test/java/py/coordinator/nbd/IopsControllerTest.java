/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
