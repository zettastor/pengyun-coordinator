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
