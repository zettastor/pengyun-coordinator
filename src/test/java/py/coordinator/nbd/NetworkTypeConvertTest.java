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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteOrder;
import org.junit.Test;
import py.test.TestBase;

public class NetworkTypeConvertTest extends TestBase {

  @Test
  public void byteBufTest() throws Exception {
    int value = ProtocoalConstants.NBD_STANDARD_MAGIC;
    final byte[] array = Util.intToByteArray(value);

    byte[] cache = new byte[4];
    ByteBuf buffer = Unpooled.wrappedBuffer(cache);
    logger.warn("order={}, {}", buffer.order(), Integer.toBinaryString(value));
    buffer.clear();
    buffer.writeInt(value);

    assertNotEquals(value, buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).readInt());
    assertEquals(value, buffer.readInt());
    assertArrayEquals(array, cache);
  }
}
