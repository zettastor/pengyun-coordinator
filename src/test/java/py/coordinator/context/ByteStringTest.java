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

package py.coordinator.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.junit.Test;
import py.client.ByteStringUtils;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.test.TestBase;

/**
 * xx.
 */
public class ByteStringTest extends TestBase {

  public ByteStringTest() throws Exception {
    init();
  }

  @Test
  public void testPosition() {
    byte[] data = NbdRequestResponseGenerator.getBuffer(10, 1);
    for (int i = 2; i < 7; i++) {
      data[i] = 2;
    }
    ByteString byteString = ByteStringUtils.newInstance(data, 2, 5);
    ByteBuffer byteBuf = byteString.asReadOnlyByteBuffer();
    logger.info("byte buf: {}", byteBuf);

    byteBuf = ByteStringUtils.getReadOnlyByteBuffer(byteString);
    logger.info("byte buf: {}", byteBuf);
    NbdRequestResponseGenerator
        .checkBuffer(byteBuf.array(), byteBuf.arrayOffset() + byteBuf.position(),
            byteBuf.remaining(), 2);
  }

  @Test
  public void testByteStringToByteBuffer() throws Exception {
    byte[] data = NbdRequestResponseGenerator.getBuffer(10, 1);
    ByteString byteString = ByteString.copyFrom(data);
    ByteBuffer byteBuffer = byteString.asReadOnlyByteBuffer();
    logger.info("has array: {}", byteBuffer.hasArray());
    Field field = ByteBuffer.class.getDeclaredField("isReadOnly");
    assertTrue(!field.isAccessible());
    field.setAccessible(true);
    field.set(byteBuffer, false);
    byte[] data1 = byteBuffer.array();
    NbdRequestResponseGenerator.checkBuffer(data1, 0, data1.length, 1);
  }

  @Test
  public void testBase() throws Exception {
    for (Annotation annotation : ByteString.class.getAnnotations()) {
      logger.info("annotation: {}", annotation);
    }

    logger.info("ByteString: {}, class: {} ", ByteString.class.getCanonicalName(),
        ByteString.class.getClass());

    for (Class<?> className : ByteString.class.getClasses()) {
      logger.info("className: {}", className);
    }
    logger.info("enclosing: {}", ByteString.class.getEnclosingClass());
    Constructor<?> constructor = ByteString.class.getEnclosingConstructor();
    logger.info("constructor: {}", constructor);
    if (ByteString.class.getEnclosingClass() != null) {
      for (Class<?> className : ByteString.class.getEnclosingClass().getClasses()) {
        logger.info("enclosing className: {}", className);
      }
    }
    logger.info("package: {}", ByteString.class.getPackage().getName());

    Class<?> class1 = Class.forName("com.google.protobuf.LiteralByteString");
    logger.info("class 1 {}", class1.getSimpleName());
    byte[] data1 = new byte[10];
    Object[] intArgs1 = new Object[]{data1};

    for (Constructor<?> constructor2 : class1.getDeclaredConstructors()) {
      logger.info("constructor: {}", constructor2);
      logger.info("accesible: {}", constructor2.isAccessible());
      for (Class<?> type : constructor2.getParameterTypes()) {
        logger.info("parameter type: {}, array: {}, {}", type, type.isArray(), type.isPrimitive());
      }
      logger.info("parameter count: {}", constructor2.getParameterTypes().length);
      constructor2.setAccessible(true);
      ByteString ff = (ByteString) constructor2.newInstance(intArgs1);
      logger.info("instance: {}", ff);
    }

    Class<?> class2 = Class.forName("com.google.protobuf.BoundedByteString");
    byte[] data2 = new byte[10];
    Object[] intArgs2 = new Object[]{data2, 2, 5};

    for (Constructor<?> constructor2 : class2.getDeclaredConstructors()) {
      logger.info("constructor: {}", constructor2);
      logger.info("accesible: {}", constructor2.isAccessible());
      logger.info("parameter count: {}", constructor2.getParameterTypes().length);
      for (Class<?> type : constructor2.getParameterTypes()) {
        logger.info("parameter type: {}, {}, array: {}, {}, {}", type, type.getSimpleName(),
            type.isArray(),
            type.isPrimitive(), byte[].class.getSimpleName());
      }
      constructor2.setAccessible(true);
      ByteString ff = (ByteString) constructor2.newInstance(intArgs2);
      logger.info("instance: {}", ff);
    }
  }

  @Test
  public void testNewInstance() {
    ByteString byteString = ByteStringUtils.newInstance(new byte[10]);
    assertNotNull(byteString);
    assertTrue(byteString.size() == 10);
    byteString = ByteStringUtils.newInstance(new byte[10], 1, 5);
    assertNotNull(byteString);
    assertTrue(byteString.size() == 5);
  }
}
