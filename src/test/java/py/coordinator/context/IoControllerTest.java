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
