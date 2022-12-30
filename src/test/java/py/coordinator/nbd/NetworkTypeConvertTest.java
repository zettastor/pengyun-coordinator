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
