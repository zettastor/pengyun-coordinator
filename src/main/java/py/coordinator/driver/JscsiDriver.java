

package py.coordinator.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.FailedToStartDriverException;


public class JscsiDriver extends Driver {

  private static final Logger logger = LoggerFactory.getLogger(JscsiDriver.class);

  public JscsiDriver(LaunchDriverParameters launchDriverParameters) {
    super(launchDriverParameters);
  }

  @Override
  public void launch() throws FailedToStartDriverException {
    logger.warn("Going to launch an jscsi driver ...");
  }

}
