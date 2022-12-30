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

package py.coordinator.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import org.apache.commons.lang.Validate;
import py.common.struct.EndPoint;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.Negotiation;
import py.coordinator.nbd.request.PydNormalRequestHeader;
import py.coordinator.nbd.request.ReplyMagic;
import py.coordinator.nbd.request.Response;


public class FakeNbdServer {

  private static PydNormalRequestHeader header = null;
  private static int bodyLen = 0;
  private static byte[] responseHeader = new byte[2 * 1024 * 1024];
  private static boolean throwErrOnReceivingRequest = false;
  private static boolean blockOnReceivingRequest = false;
  private static boolean disconnectOnReceivingRequest = false;
  private static boolean replyUncognizedDataOnReceivingRequest = false;
  private static boolean noReply = false;

  public FakeNbdServer() {
  }


  
  public static void replyHeader(Socket socket) {
    ReplyMagic replyMagic = header.getMagicType().getReplyMagic();
    Response response = new Response(replyMagic, 0, header.getHandler(), null);
    int length = replyMagic.getReplyLength();
    if (header.getRequestType().isRead()) {
      length += header.getLength();
    }

    ByteBuf tmp = Unpooled.wrappedBuffer(responseHeader);
    tmp.clear();
    response.writeTo(tmp);
    tmp.release();

    try {
      socket.getOutputStream().write(responseHeader, 0, length);
    } catch (Exception e) {
      System.out.println("can not send for io={}" + header);
    }
  }

  private static int parseWrite(Socket socket, byte[] buffer, int length) {
    int offset = 0;
    int leftSize = length;

    while (offset < length) {
      if (header != null) {
        if (leftSize + bodyLen < header.getLength()) {
          bodyLen += leftSize;
          return 0;
        } else {
         
          offset += (header.getLength() - bodyLen);
          leftSize -= (header.getLength() - bodyLen);
          replyHeader(socket);
          header = null;
          bodyLen = 0;
          continue;
        }
      }

      if (leftSize < 32) {
        System.out.println("left size " + leftSize);
        System.arraycopy(buffer, offset, buffer, 0, leftSize);
        return leftSize;
      } else {
        ByteBuf buf = Unpooled.wrappedBuffer(buffer, offset, leftSize);
        MagicType magicType = MagicType.findByValue(buf.readInt());
        Validate.isTrue(magicType == MagicType.PYD_NORMAL);
        header = new PydNormalRequestHeader(buf);
        buf.release();
        offset += 32;
        leftSize -= 32;

        NbdRequestType type = header.getRequestType();
       
        if (type.isWrite()) {
          if (leftSize < header.getLength()) {
            bodyLen = leftSize;
            return 0;
          } else {
            offset += (header.getLength() - bodyLen);
            leftSize -= (header.getLength() - bodyLen);
            replyHeader(socket);
            header = null;
            bodyLen = 0;
            continue;
          }
        } else {
          replyHeader(socket);
          header = null;
          bodyLen = 0;
        }
      }
    }

    return 0;
  }


  
  public static void main(String[] args) throws Exception {

    final EndPoint serverPort = new EndPoint("100.100.0.10", 6666);
    Thread thread = new Thread("happy") {
      public void run() {
        Socket client = null;
        try {
          ServerSocket serverSocket = new ServerSocket(serverPort.getPort());
         
          client = serverSocket.accept();
        } catch (Exception e) {
          System.out.println("caught an exception when listening" + e);
        }

       
        int bufferSize = Negotiation.getNegotiateLength();
        byte[] buff = new byte[bufferSize];
        ByteBuf byteBuf = Unpooled.wrappedBuffer(buff);
        byteBuf.clear();
        Negotiation negotiation = MagicType.generateNegotiation(100L * 1024L * 1024L * 1024L);
        System.out.println("negotiation=" + negotiation);
        negotiation.writeTo(byteBuf);
        byteBuf.release();

        try {
          client.getOutputStream().write(buff);
        } catch (Exception e) {
          System.out.println("fail to negotiation" + e);
          System.exit(0);
        }

        byte[] rcvBuff = new byte[128 * 1024];
        int offset = 0;
        try {
          while (true) {
            int count = client.getInputStream().read(rcvBuff, offset, rcvBuff.length - offset);
            if (count < 0) {
              System.out.println("caught an exception when receiving nu=" + count);
              System.exit(0);
            }
            offset = parseWrite(client, rcvBuff, count + offset);
          }
        } catch (Exception e) {
          System.out.println("caught an exception when receiving " + e);
        }
      }
    };

    thread.start();

    Scanner scanner = new Scanner(System.in);

    while (true) {
      try {
        System.out.println("Select an option from below:");
        System.out.println("\t[0]All right");
        System.out.println("\t[1]Throw an error to client on receiving request");
        System.out.println("\t[2]Block server on receiving request");
        System.out.println("\t[3]Disconnect with client on receiving request");
        System.out.println("\t[4]Reply uncognized data to client on receiving request");
        System.out.println("\t[5]No reply");
        System.out.print("Your option: ");
        int option = scanner.nextInt();

        switch (option) {
          case 0:
            blockOnReceivingRequest = false;
            disconnectOnReceivingRequest = false;
            throwErrOnReceivingRequest = false;
            replyUncognizedDataOnReceivingRequest = false;
            noReply = false;
            System.out.println(">>> All right!");
            break;
          case 1:
            blockOnReceivingRequest = false;
            disconnectOnReceivingRequest = false;
            throwErrOnReceivingRequest = true;
            replyUncognizedDataOnReceivingRequest = false;
            noReply = false;
            System.out.println(">>> Throw an error to client on receiving request!");
            break;
          case 2:
            blockOnReceivingRequest = true;
            replyUncognizedDataOnReceivingRequest = false;
            noReply = false;
            System.out.println(">>> Block server on receiving request!");
            break;
          case 3:
            blockOnReceivingRequest = false;
            disconnectOnReceivingRequest = true;
            noReply = false;
            replyUncognizedDataOnReceivingRequest = false;
            System.out.println(">>> Disconnect with client on receiving request!");
            break;
          case 4:
            blockOnReceivingRequest = false;
            disconnectOnReceivingRequest = false;
            throwErrOnReceivingRequest = false;
            replyUncognizedDataOnReceivingRequest = true;
            noReply = false;
            System.out.println(">>> Reply uncognized data to client on receiving request!");
            break;
          case 5:
            blockOnReceivingRequest = false;
            disconnectOnReceivingRequest = false;
            throwErrOnReceivingRequest = false;
            replyUncognizedDataOnReceivingRequest = false;
            noReply = true;
            System.out.println(">>> No reply!");
            break;
          default:
            System.out.println(">>> Invalid input!");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}