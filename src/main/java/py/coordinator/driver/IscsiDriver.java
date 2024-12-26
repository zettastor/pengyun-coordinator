
package py.coordinator.driver;

import org.apache.commons.lang3.NotImplementedException;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.drivercontainer.exception.FailedToStartDriverException;


@Deprecated
public class IscsiDriver extends Driver {

  public IscsiDriver(LaunchDriverParameters launchDriverParameters) {
    super(launchDriverParameters);
  }

  @Override
  public void launch() throws FailedToStartDriverException {
    throw new NotImplementedException("Not implement abstract method 'launch' yet");
  }
}
