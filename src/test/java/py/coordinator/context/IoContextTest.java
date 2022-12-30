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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import py.buffer.PyBuffer;
import py.common.RequestIdBuilder;
import py.coordinator.base.IoContextGenerator;
import py.coordinator.base.NbdRequestResponseGenerator;
import py.coordinator.iorequest.iorequest.IoRequest;
import py.coordinator.iorequest.iounit.IoUnit;
import py.coordinator.iorequest.iounitcontext.IoUnitContext;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.request.Reply;
import py.test.TestBase;

/**
 * xx.
 */
public class IoContextTest extends TestBase {

  private Long volumeId = RequestIdBuilder.get();
  private List<Reply> replies = new ArrayList<>();
  NbdResponseSender sender = new NbdResponseSender() {
    @Override
    public void send(Reply reply) {
      logger.warn("reply request: {}", reply.getResponse());
      replies.add(reply);
    }
  };

  public IoContextTest() throws Exception {
    super.init();
  }

  @Test
  public void testReadReply() throws Exception {
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, 1000, 10, 1);
    ioContextGenerator.setSender(sender);
    List<IoUnitContext> contexts = ioContextGenerator.generateReadIoContexts(volumeId, 0, 12);
    IoUnit ioUnit;
    IoRequest ioRequest = contexts.get(0).getIoRequest();

    // deal with the first context
    assertEquals(ioRequest.getReferenceCount(), 2);
    ioUnit = contexts.get(0).getIoUnit();
    final int ioUnitLength1 = ioUnit.getLength();
    ioUnit.setSuccess(true);
    ioUnit.setPyBuffer(new PyBuffer(NbdRequestResponseGenerator.getByteBuf(ioUnit.getLength(), 1)));
    contexts.get(0).done();
    assertEquals(ioRequest.getReferenceCount(), 1);

    // deal with the next context
    ioUnit = contexts.get(1).getIoUnit();
    final int ioUnitLength2 = ioUnit.getLength();
    ioUnit.setSuccess(true);
    ioUnit.setPyBuffer(new PyBuffer(NbdRequestResponseGenerator.getByteBuf(ioUnit.getLength(), 2)));
    contexts.get(1).done();
    assertEquals(ioRequest.getReferenceCount(), 0);
    assertEquals(1, replies.size());

    ByteBuf byteBuf;
    byteBuf = replies.get(0).getResponse().getBody().readSlice(ioUnitLength1);
    NbdRequestResponseGenerator.checkBuffer(byteBuf, 1);

    byteBuf = replies.get(0).getResponse().getBody().readSlice(ioUnitLength2);
    NbdRequestResponseGenerator.checkBuffer(byteBuf, 2);
  }

  @Test
  public void testWriteReply() throws Exception {
    IoContextGenerator ioContextGenerator = new IoContextGenerator(volumeId, 1000, 10, 1);
    ioContextGenerator.setSender(sender);
    List<IoUnitContext> contexts = ioContextGenerator.generateWriteIoContexts(volumeId, 0, 12);
    IoRequest ioRequest = contexts.get(0).getIoRequest();

    // deal with the first request
    assertEquals(ioRequest.getReferenceCount(), 2);
    contexts.get(0).getIoUnit().setSuccess(false);
    contexts.get(0).getIoUnit().setPyBuffer(null);
    contexts.get(0).done();
    assertEquals(ioRequest.getReferenceCount(), 1);

    contexts.get(1).getIoUnit().setSuccess(false);
    contexts.get(1).getIoUnit().setPyBuffer(null);
    contexts.get(1).done();

    assertEquals(ioRequest.getReferenceCount(), 0);
    assertEquals(1, replies.size());
    assertEquals(replies.get(0).getResponse().getErrCode(), ProtocoalConstants.EIO);
  }
}
