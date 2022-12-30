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

package py.coordinator.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import py.app.Launcher;
import py.coordinator.configuration.CoordinatorServiceConfig;
import py.driver.DriverMetadata;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.processmanager.Pmdb;
import py.processmanager.utils.PmUtils;


public class DriverLauncher extends Launcher {

  public static final long randomNumberMarker = 4252063745L;
  public static final String LAUNCH_FAILED = String.format("[%s]fail", randomNumberMarker);
  public static final String LAUNCH_SUCCEEDED = String.format("[%s]success", randomNumberMarker);
  private static Logger logger = LoggerFactory.getLogger(DriverLauncher.class);
  private ApplicationContext appContext;

  private String[] args;


  
  public DriverLauncher(String beansHolder, String serviceRunningPath, String[] args) {
    super(beansHolder, serviceRunningPath);

    this.args = args;
    logger.warn("args: {}", Arrays.asList(args));
  }

  
  public static void main(String[] args) {
    logger.info("going to create dir for pid file save");

   
    if (args.length == 0) {
      logger.error(
          "Can't launch driver, no arguments are specified. At lease a driver type has to be "
              + "specified");
      outputFailure();
      System.exit(1);
    }

   
   

    Launcher launcher = new DriverLauncher("", "", args);
    launcher.launch();
  }

  
  private static void outputFailure() {
   
   
    System.out.println();
    System.out.println(LAUNCH_FAILED);
  }

  
  private static void outputSuccess(int processId) {
   
   
    System.out.println();
    System.out.println(encodeProcessId(processId));
  }

  public static String encodeProcessId(int processId) {
    return String.format("%s-%s", LAUNCH_SUCCEEDED, Integer.toString(processId));
  }

  /**
   * 0: could not get process id.
   */
  public static int decodeProcessid(String string) {
    if (!string.contains(LAUNCH_SUCCEEDED)) {
      return 0;
    }
    return Integer.valueOf(string.split("-")[1]);
  }


  
  public static boolean processLaunchResults(BufferedReader reader) {
    if (reader == null) {
      logger.error("Reader is null");
      return false;
    }

    String lineFromProcess = null;
    StringBuilder outputFromProcessd = new StringBuilder();
    boolean succeeded = false;
    do {
      try {
        logger.debug("reading the processing result of driver launcher");
        lineFromProcess = reader.readLine();
        outputFromProcessd.append(lineFromProcess);
      } catch (IOException e) {
        logger.error("Failed to read process builder results", e);
        break;
      }

      if (lineFromProcess == null) {
        logger.error(
            "the launch results can't be null before \"failed\" or \"succeeded\" strings are "
                + "returned");
        break;
      }

      if (lineFromProcess.equals(DriverLauncher.LAUNCH_SUCCEEDED)) {
        logger.debug("Successfully launched driver");
        succeeded = true;
        break;
      } else if (lineFromProcess.equals(DriverLauncher.LAUNCH_FAILED)) {
        logger.error("Failed to launch driver");
        break;
      }
    } while (true);

    logger.warn("the output from process builder is {}", outputFromProcessd.toString());
    // close reader
    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("can't close the input reader reading results from sub-process", e);
    }
    return succeeded;
  }


  
  public static void deletePidFileByVolumeId(long volumeId) throws Exception {
    String dir =
        System.getProperty("user.dir") + "/" + Pmdb.SERVICE_PID_NAME + "_coordinator" + "/";
    String filePath = dir + volumeId;
    File file = new File(filePath);
    logger.debug("try to delete volume_file {}", volumeId);
    try {
      if (file.exists()) {
        file.delete();
      }
    } catch (Exception e) {
      logger.error("Failed to delete file by its process id", e);
    }
  }

  
  @Override
  protected ApplicationContext genAppContext() throws Exception {
    // custom command line property source
    SimpleCommandLinePropertySource propertySource = new SimpleCommandLinePropertySource(args);

    //parse param from commandline and apply it to driver app config
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.getEnvironment().getPropertySources().addFirst(propertySource);
    logger.warn("args:{}, volumeId:{}", Arrays.asList(args),
        propertySource.getProperty("driver.backend.volume.id"));
    context.register(CoordinatorServiceConfig.class);
    context.refresh();

    String currentUsingProfiles = System.getProperty("metric.enable.profiles");
    logger.warn("current metric enable profiles:{}", currentUsingProfiles);

    super.genAppContext();

    return context;
  }

  @Override
  public void launch() {
    try {
      //parse param from commandline
      appContext = genAppContext();
      LaunchDriverParameters launchDriverParameters = appContext
          .getBean(LaunchDriverParameters.class);

      DriverExecutorService driverExecutorService = appContext.getBean(DriverExecutorService.class);
      //going to start the driver whose type specified in driver configuration
      driverExecutorService.execute();

      int currentProcessId = PmUtils.getCurrentProcessPid();
      // we use volume id as file name, backup process id to file
      logger.warn(
          "check driver launch at:[{}:{}] already startup, output message to notice driver "
              + "container",
          launchDriverParameters.getHostName(), launchDriverParameters.getPort());
      outputSuccess(currentProcessId);

      /*
       * After driver give process id of self to driver container, the driver will check if the
       * process id is
       * stored in file, if not exit the process otherwise the driver will become orphan process.
       */
      final int tryAmount = 10;
      int tryCount = 0;
      File file = new File(System.getProperty("user.dir"));
      Path path = Paths.get(file.toString());
     
      String varPath = path.getParent().getParent().getParent().toString();

      for (; tryCount < tryAmount; tryCount++) {
        DriverMetadata driver = DriverMetadata
            .buildFromFile(Paths.get(varPath, Pmdb.COORDINATOR_PIDS_DIR_NAME,
                Long.toString(launchDriverParameters.getVolumeId()),
                launchDriverParameters.getDriverType().name(),
                Integer.toString(launchDriverParameters.getSnapshotId())));
        if (driver.getProcessId() == currentProcessId) {
          logger.warn("Driver launched at:[{}:{}] on volume id:{} start up successfully",
              launchDriverParameters.getHostName(), launchDriverParameters.getPort(),
              driver.getVolumeId());
          break;
        }

        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          logger.warn("Caught an exception", e);
        }
      }

      if (tryCount >= tryAmount) {
        logger.error("Timeout for driver container to backup process id {} to file",
            currentProcessId);
        System.exit(1);
      }
    } catch (Exception e) {
      logger.error("Caught an exception when launch driver", e);
      outputFailure();
      System.exit(1);
    }
  }

  @Override
  protected void backupServiceProcessId() {
   
   
  }

  @Override
  protected void startAppEngine(ApplicationContext appContext) {
   

  }

}
