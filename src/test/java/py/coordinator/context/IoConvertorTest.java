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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;
import py.common.RequestIdBuilder;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iorequest.IoRequestImpl;
import py.coordinator.iorequest.iorequest.IoRequestType;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.lib.DiscardInfo;
import py.coordinator.lib.IoConvertorImpl;
import py.coordinator.lib.NbdAsyncIoCallBack;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.RequestHeader;
import py.test.TestBase;

/**
 * xx.
 */
public class IoConvertorTest extends TestBase {

  public long volumeId = RequestIdBuilder.get();
  private Channel channel = mock(Channel.class);
  private NbdResponseSender sender = mock(NbdResponseSender.class);

  public IoConvertorTest() throws Exception {
    super.init();
  }

  @Test
  public void runSeveralPage() throws Exception {
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, 1000, 10);
    // read
    List<IoUnitContext> contexts;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 9, 20);
    assertTrue(contexts.size() == 3);
    assertTrue(contexts.get(0).getIoUnit().getLength() == 1);
    assertTrue(contexts.get(1).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(2).getIoUnit().getLength() == 9);

    // write
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 9, 20);
    assertTrue(contexts.size() == 3);
    assertTrue(contexts.get(0).getIoUnit().getLength() == 1);
    assertTrue(contexts.get(1).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(2).getIoUnit().getLength() == 9);

    // write can check data
    int delta = 5;
    contexts = ioContextGenerator
        .generateWriteIoContexts(volumeId, 3, 20, NbdRequestResponseGenerator.getBuffer(20, delta));
    assertTrue(contexts.size() == 3);
    assertTrue(contexts.get(0).getIoUnit().getLength() == 7);
    assertTrue(contexts.get(1).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(2).getIoUnit().getLength() == 3);

    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(0).getIoUnit().getPyBuffer().getByteBuffer(), delta);
    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(1).getIoUnit().getPyBuffer().getByteBuffer(), delta);
    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(2).getIoUnit().getPyBuffer().getByteBuffer(), delta);
  }

  @Test
  public void runSeveralSegment() throws Exception {
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, 1000, 10, 2);
    List<IoUnitContext> contexts;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 9, 20);
    assertTrue(contexts.size() == 3);

    assertTrue(contexts.get(0).getIoUnit().getLength() == 1 && contexts.get(0).getSegIndex() == 0);
    assertTrue(contexts.get(1).getIoUnit().getLength() == 10 && contexts.get(1).getSegIndex() == 0);
    assertTrue(contexts.get(2).getIoUnit().getLength() == 9 && contexts.get(2).getSegIndex() == 1);

    int delta = 5;
    contexts = ioContextGenerator
        .generateWriteIoContexts(volumeId, 3, 20, NbdRequestResponseGenerator.getBuffer(20, delta));
    assertTrue(contexts.size() == 3);
    assertTrue(contexts.get(0).getIoUnit().getLength() == 7 && contexts.get(0).getSegIndex() == 0);
    assertTrue(contexts.get(1).getIoUnit().getLength() == 10 && contexts.get(1).getSegIndex() == 0);
    assertTrue(contexts.get(2).getIoUnit().getLength() == 3 && contexts.get(2).getSegIndex() == 1);

    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(0).getIoUnit().getPyBuffer().getByteBuffer(), delta);
    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(1).getIoUnit().getPyBuffer().getByteBuffer(), delta);
    NbdRequestResponseGenerator
        .checkBuffer(contexts.get(2).getIoUnit().getPyBuffer().getByteBuffer(), delta);
  }

  @Test
  public void runExtendSegment() throws Exception {
    long segmentSize = 1000;
    int pageSize = 10;
    int pageCount = 2;

    List<Long> layoutVolume = new ArrayList<Long>();
    layoutVolume.add(3L);
    layoutVolume.add(5L);
    layoutVolume.add(2L);
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, segmentSize, pageSize,
        pageCount, layoutVolume);

    // read
    List<IoUnitContext> contexts = null;
    int size = 5 * pageSize;
    contexts = ioContextGenerator.generateReadIoContexts(volumeId, 3000 - 24, size);
    assertTrue(contexts.size() == 6);
    assertTrue(contexts.get(0).getSegIndex() == 1 && contexts.get(0).getIoUnit().getLength() == 4);
    assertTrue(contexts.get(1).getSegIndex() == 2 && contexts.get(1).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(2).getSegIndex() == 2 && contexts.get(2).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(3).getSegIndex() == 3 && contexts.get(3).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(4).getSegIndex() == 3 && contexts.get(4).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(5).getSegIndex() == 4 && contexts.get(5).getIoUnit().getLength() == 6);

    // write
    size = 5 * pageSize;
    contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 3000 - 24, size);
    assertTrue(contexts.size() == 6);
    assertTrue(contexts.get(0).getSegIndex() == 1 && contexts.get(0).getIoUnit().getLength() == 4);
    assertTrue(contexts.get(1).getSegIndex() == 2 && contexts.get(1).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(2).getSegIndex() == 2 && contexts.get(2).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(3).getSegIndex() == 3 && contexts.get(3).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(4).getSegIndex() == 3 && contexts.get(4).getIoUnit().getLength() == 10);
    assertTrue(contexts.get(5).getSegIndex() == 4 && contexts.get(5).getIoUnit().getLength() == 6);
  }

  @Test
  public void discardCheckTest() throws Exception {
    /* set the value ******************/
    final Map<Long, Map<Integer, List<DiscardInfo>>> mapForDiscard = new ConcurrentHashMap<>();
    final Map<Integer, List<DiscardInfo>> mapPyh = new ConcurrentHashMap<>();
    List<DiscardInfo> discardInfos = new ArrayList<>();
    final List<DiscardInfo> discardInfos2 = new ArrayList<>();
    final List<DiscardInfo> discardInfos3 = new ArrayList<>();
    long offset1 = 125;
    long offset2 = 256;
    long offset3 = 390;
    final long offset4 = 512;
    final long offset5 = 640;
    long offset6 = 128;

    int len1 = 3;
    int len2 = 128;
    int len3 = 100;
    final int len4 = 125;

    final int lenCheck1 = 259;
    final int lenCheck2 = 100;
    final int lenCheck3 = 125;
    final int lenCheck4 = 128;
    final int lenCheck5 = 259;

    discardInfos.add(new DiscardInfo(offset1, len1));
    discardInfos.add(new DiscardInfo(offset6, len2));
    discardInfos.add(new DiscardInfo(offset2, len2));
    discardInfos.add(new DiscardInfo(offset3, len3));
    mapPyh.put(0, discardInfos);

    discardInfos2.add(new DiscardInfo(offset4, len4));
    discardInfos2.add(new DiscardInfo(offset5, len2));
    mapPyh.put(1, discardInfos2);

    discardInfos3.add(new DiscardInfo(offset1, len1));
    discardInfos3.add(new DiscardInfo(offset6, len2));
    discardInfos3.add(new DiscardInfo(offset2, len2));
    mapPyh.put(3, discardInfos3);

    mapForDiscard.put(1L, mapPyh);

    /* check the value ************************/
    long offset = 9L;
    long len = 20L;
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, 1000, 10, 2);
    IoConvertorImpl ioConvertor = ioContextGenerator.getIoConvertor();
    RequestHeader requestHeader = new PydNormalRequestHeader(NbdRequestType.Discard, 0, offset,
        (int) len, 0);
    ByteBuf body = null;
    NbdAsyncIoCallBack nbdAsyncIoCallBack = new NbdAsyncIoCallBack(channel, requestHeader, sender);

    IoRequest ioRequest = new IoRequestImpl(offset, len, IoRequestType.Discard, body,
        nbdAsyncIoCallBack, 1);
    ioRequest.setVolumeId(volumeId);
    ioConvertor.checkDiscardInfo(1, ioRequest, mapForDiscard);

    List<IoUnit> units = ioRequest.getIoUnits();
    assertEquals(units.size(), 5);
    for (int i = 0; i < units.size(); i++) {
      logger.warn("the offset :{}  the length is :{}", units.get(i).getOffset(),
          units.get(i).getLength());
    }
    assertTrue(units.get(0).getOffset() == offset1 && units.get(0).getLength() == lenCheck1);
    assertTrue(units.get(1).getOffset() == offset3 && units.get(1).getLength() == lenCheck2);
    assertTrue(units.get(2).getOffset() == offset4 && units.get(2).getLength() == lenCheck3);
    assertTrue(units.get(3).getOffset() == offset5 && units.get(3).getLength() == lenCheck4);
    assertTrue(units.get(4).getOffset() == offset1 && units.get(4).getLength() == lenCheck5);

  }
}
