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

package py.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import py.coordinator.lib.Coordinator;


public class Launcher extends py.app.Launcher {

  private static Logger logger = LoggerFactory.getLogger(Launcher.class);
  private Coordinator coordinator;

  public Launcher(String beansHolder, String serviceRunningPath) {
    super(beansHolder, serviceRunningPath);
  }

  @Override
  public void startAppEngine(ApplicationContext appContext) {
    try {
      CoordinatorAppEngine engine = appContext.getBean(CoordinatorAppEngine.class);
      engine.getCoordinatorImpl().setCoordinator(coordinator);
      logger.info("Coordinator App Engine get Max Network Frame Size is {}",
          engine.getMaxNetworkFrameSize());
      engine.start();
    } catch (Exception e) {
      logger.error("Caught an exception when start dih service", e);
      System.exit(1);
    }
  }


  @Override
  protected ApplicationContext genAppContext() throws Exception {

    if (beansHolder.contains(".xml")) {
      ApplicationContext context = new ClassPathXmlApplicationContext(beansHolder);
      return context;
    } else if (beansHolder.contains(".class")) {
      logger.debug("beanHolder : {}", beansHolder);
      int postfixPos = beansHolder.indexOf(".class");
      String className = beansHolder.substring(0, postfixPos);
      Class contextClass = Class.forName(className);

      ApplicationContext context = new AnnotationConfigApplicationContext(contextClass);

      logger.debug("app context:{}", context);
      return context;
    }

    return null;
  }

  public Coordinator getCoordinator() {
    return coordinator;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

}
