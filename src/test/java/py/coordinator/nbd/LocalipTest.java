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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import py.common.Utils;

/**
 * xx.
 */
public class LocalipTest {


  /**
   * xx.
   */
  public static List<String> getAllLocalHost() {
    List<String> localHostList = new ArrayList<String>();

    Enumeration<?> allNetInterfaces = null;
    try {
      allNetInterfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      return localHostList;
    }
    if (allNetInterfaces == null) {
      return localHostList;
    }

    InetAddress ip = null;
    while (allNetInterfaces.hasMoreElements()) {
      NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
      Enumeration<?> addresses = netInterface.getInetAddresses();
      while (addresses.hasMoreElements()) {
        ip = (InetAddress) addresses.nextElement();
        if (ip != null && ip instanceof Inet4Address) {
          localHostList.add(ip.getHostAddress());
        }
      }
    }
    return localHostList;
  }

  @Test
  public void test() {
    for (String localHost : getAllLocalHost()) {
      Assert.assertEquals(true, Utils.isLocalIpAddress(localHost));
    }
  }
}
