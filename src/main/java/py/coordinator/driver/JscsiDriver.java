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
