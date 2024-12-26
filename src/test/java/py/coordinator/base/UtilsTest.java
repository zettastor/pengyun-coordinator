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
