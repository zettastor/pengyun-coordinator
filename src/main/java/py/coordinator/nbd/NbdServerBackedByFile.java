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

import java.net.InetAddress;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import py.common.struct.EndPoint;
import py.coordinator.backends.FileStorage;
import py.coordinator.configuration.NbdConfiguration;


public class NbdServerBackedByFile {

  private static final Logger logger = Logger.getLogger(NbdServerBackedByFile.class);

  private static void initLogs() {
    PatternLayout layout = new PatternLayout();
    String conversionPattern = "%-5p[%d][%t]%C(%L):%m%n";
    layout.setConversionPattern(conversionPattern);


    RollingFileAppender rollingFileAppender = new RollingFileAppender();
    rollingFileAppender.setFile("logs/datanode-integtest.log");
    rollingFileAppender.setLayout(layout);
    rollingFileAppender.setThreshold(Level.DEBUG);
    rollingFileAppender.setMaxBackupIndex(10);
    rollingFileAppender.setMaxFileSize("400MB");
    rollingFileAppender.activateOptions();


    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setLayout(layout);
    consoleAppender.setThreshold(Level.INFO);
    consoleAppender.setTarget("System.out");
    consoleAppender.setEncoding("UTF-8");
    consoleAppender.activateOptions();


    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(rollingFileAppender);
    rootLogger.addAppender(consoleAppender);
  }



  public static void main(String[] args) throws Exception {
    initLogs();
    logger.info("Starting a nbd server");
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.INFO);

    String fileName = "/tmp/nbdFile";
    FileStorage storage = new FileStorage(fileName);
    storage.setConfig(fileName, 1 * 200L * 1024L * 1024L);

    NbdConfiguration nbdConfig = new NbdConfiguration();
    nbdConfig.setEndpoint(new EndPoint(InetAddress.getLocalHost().getHostAddress(), 12340));
    PydClientManager pydClientManager = new PydClientManager(
        nbdConfig.getHeartbeatTimeIntervalAfterIoRequestMs(),
        false, nbdConfig.getReaderIdleTimeoutSec(), storage);

    NbdServer server = new NbdServer(nbdConfig, storage, pydClientManager);
    server.start();
  }
}
