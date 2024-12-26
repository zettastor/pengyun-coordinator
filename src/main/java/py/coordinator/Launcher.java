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
