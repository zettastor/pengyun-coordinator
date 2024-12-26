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

/**
 * xx.
 */
public final class ProtocoalConstants {

  public static final byte[] CLI_SERV_MAGIC = new byte[]{0x00, 0x00, 0x42, 0x02, (byte) (0x81),
      (byte) (0x86), 0x12,
      0x53};
  public static final byte[] INIT_PASSWD = new byte[]{'N', 'B', 'D', 'M', 'A', 'G', 'I',
      'C'};
  public static final byte[] NO_FLAG = new byte[]{0x00, 0x00, 0x00, 0x00};
  public static final byte[] PYD_VERSION_INFO = new byte[]{1};
  public static final byte[] NBD_VERSION_INFO = new byte[]{0};
 
  public static final byte[] ZERO_BLOCK = Util.zeros(123 - 4);
  
  public static final int NBD_STANDARD_MAGIC = 0x25609513;
  public static final int NBD_STANDARD_METRIC_MAGIC = 0x25609514;
  public static final int NBD_IOSUM_MAGIC = 0x25609515;
  public static final int NBD_IOSUM_METRIC_MAGIC = 0x25609516;
  public static final int NBD_REPLY_MAGIC = 0x67446698;
  public static final int NBD_METRIC_REPLY_MAGIC = 0x67446699;
  public static final int NBD_HEARTBEAT_MAGIC = 0x67446697;
 
  public static final int SUCCEEDED = 0;
  
  public static final int EINTR = 4;
  public static final int EIO = 5;
  public static final int ENXIO = 6;
  public static final int ENOEXEC = 8;
  public static final int EAGAIN = 11;
  public static final int EFAULT = 14;
  public static final int EBUSY = 16;
  public static final int EINVAL = 22;
  public static final int ENOSPC = 28;
  public static final int EROFS = 30;

  private ProtocoalConstants() {
   
  }
}