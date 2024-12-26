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
