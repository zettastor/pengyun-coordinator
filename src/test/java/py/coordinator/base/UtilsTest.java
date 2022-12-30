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

package py.coordinator.base;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import py.informationcenter.AccessPermissionType;
import py.test.TestBase;

public class UtilsTest extends TestBase {

  @Test
  public void testCompareAndSet() {
    AtomicReference<AccessPermissionType> testAccess = new AtomicReference<>();
    AccessPermissionType type1 = AccessPermissionType.READ;
    boolean setFlag = testAccess.compareAndSet(null, type1);
    Assert.assertEquals(testAccess.get(), type1);
    Assert.assertTrue(setFlag);

    AccessPermissionType type2 = AccessPermissionType.READWRITE;
    setFlag = testAccess.compareAndSet(null, type2);
    Assert.assertEquals(testAccess.get(), type1);
    Assert.assertTrue(!setFlag);
  }

  @Test
  public void testRemoveFromMapWhileLooping() {
    Map<Integer, Long> testMap = new ConcurrentHashMap<>();
    testMap.put(0, 10L);
    testMap.put(1, 11L);
    testMap.put(2, 12L);
    testMap.put(3, 13L);
    testMap.put(4, 14L);

    Iterator<Map.Entry<Integer, Long>> iterator = testMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Integer key = iterator.next().getKey();
      testMap.remove(key);
    }

    Assert.assertEquals(0, testMap.size());
  }

}
