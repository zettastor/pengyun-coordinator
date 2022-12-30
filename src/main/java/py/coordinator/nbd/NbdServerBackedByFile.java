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
