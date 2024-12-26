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

package py.coordinator.utils;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Log4jProperties;
import py.common.struct.EndPoint;
import py.coordinator.backends.FileStorage;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.nbd.NbdServer;
import py.coordinator.nbd.PydClientManager;
import py.exception.StorageException;
import py.informationcenter.AccessPermissionType;


public class NbdDriverLauncher {

  public static final int nbdLauncherPort = 1234;
  private static final Logger logger = LoggerFactory.getLogger(NbdDriverLauncher.class);



  public static void main(String[] args) throws Exception {

    Properties log4jProperties = Log4jProperties.getProperties("logs/nbd-server.log", Level.WARN);
    PropertyConfigurator.configure(log4jProperties);


    if (args.length != 1) {
      logger.error("Please allow an remote client to connect in by specifing an ip address");
      System.exit(1);
    }


    String onlyRemoteClientIpAddress = args[0];
    try {
      InetAddresses.forString(onlyRemoteClientIpAddress);
    } catch (IllegalArgumentException e) {
      logger.error("Invalid input of ip address, please try again");
      System.exit(1);
    }


    String fileName = "/tmp/nbdFile";
    FileStorage fileStorage = new FileStorage(fileName);
    fileStorage.setConfig(fileName, 1 * 200L * 1024L * 1024L);

    try {
      fileStorage.open();
    } catch (StorageException e) {
      logger.error("Failed to start coordinator instance", e);
      System.exit(1);
    }


    Map<String, AccessPermissionType> accessRuleTable = new HashMap<String, AccessPermissionType>();
    accessRuleTable.put(onlyRemoteClientIpAddress, AccessPermissionType.READWRITE);

    AnotherNbdConfiguration nbdConfig = new AnotherNbdConfiguration();
    nbdConfig.setVolumeId(1L);
    nbdConfig
        .setEndpoint(new EndPoint(InetAddress.getLocalHost().getHostAddress(), nbdLauncherPort));
    nbdConfig.setVolumeAccessRules(accessRuleTable);
    PydClientManager pydClientManager = new PydClientManager(
        nbdConfig.getHeartbeatTimeIntervalAfterIoRequestMs(),
        false, nbdConfig.getReaderIdleTimeoutSec(), fileStorage);

    final NbdServer server = new NbdServer(nbdConfig, fileStorage, pydClientManager);
    try {
      logger.info("Now start nbd server whose backend file is {} on port {}", fileName,
          nbdLauncherPort);
      server.start();
    } catch (Exception e) {
      logger.error("Failed to launch nbd driver", e);
      System.exit(1);
    }

  }

  public static class AnotherNbdConfiguration extends NbdConfiguration {

    private Map<String, AccessPermissionType> volumeAccessRules;

    @Override
    public Map<String, AccessPermissionType> getVolumeAccessRules() {
      return volumeAccessRules;
    }

    public void setVolumeAccessRules(Map<String, AccessPermissionType> volumeAccessRules) {
      this.volumeAccessRules = volumeAccessRules;
    }
  }
}
