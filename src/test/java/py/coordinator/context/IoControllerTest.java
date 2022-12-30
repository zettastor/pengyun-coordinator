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

package py.coordinator.context;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.periodic.UnableToStartException;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.test.TestBase;

/**
 * xx.
 */
public class IoControllerTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(IoControllerTest.class);

  private PeriodicWorkExecutorImpl upperIopsLimitExcuter;
  private Integer puttingRate = 200;
  private TestValue value;

  @Test
  public void testThreadCommunication() throws UnableToStartException, InterruptedException {

    value = new TestValue(100);

    upperIopsLimitExcuter = new PeriodicWorkExecutorImpl();
    ExecutionOptionsReader putTokenExecutionOptionReader = new ExecutionOptionsReader(1, 1,
        puttingRate, null);
    TestWorkerFactory testWorkerFactory = new TestWorkerFactory(value);

    upperIopsLimitExcuter.setExecutionOptionsReader(putTokenExecutionOptionReader);
    upperIopsLimitExcuter.setWorkerFactory(testWorkerFactory);

    upperIopsLimitExcuter.start();
    Thread.sleep(1000);
    value.setValue(200);
    Thread.sleep(1000);
    upperIopsLimitExcuter.stopNow();
    Thread.sleep(1000);
  }

  class TestWorkerFactory implements WorkerFactory {

    private TestValue value;

    public TestWorkerFactory(TestValue value) {
      super();
      this.value = value;
    }

    @Override
    public Worker createWorker() {
      TestWorker worker = new TestWorker(value);
      return worker;
    }

  }

  class TestWorker implements Worker {

    private TestValue value;

    public TestWorker(TestValue value) {
      super();
      this.value = value;
    }

    @Override
    public void doWork() throws Exception {
      Thread.currentThread().setName("hehe-Thread");
      logger.debug("value : {}", value.getValue());
    }

  }

  class TestValue {

    private long value;

    public TestValue(long value) {
      super();
      this.value = value;
    }

    public long getValue() {
      return value;
    }

    public void setValue(long value) {
      this.value = value;
    }

  }


}
