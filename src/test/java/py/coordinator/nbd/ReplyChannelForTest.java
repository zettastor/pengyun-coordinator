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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * xx.
 */
public class ReplyChannelForTest implements Channel {

  private List<ByteBuf> replyList = new ArrayList<ByteBuf>();

  public List<ByteBuf> getReplyList() {
    return replyList;
  }

  public void setReplyList(List<ByteBuf> replyList) {
    this.replyList = replyList;
  }

  @Override
  public <T> Attribute<T> attr(AttributeKey<T> key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
    return false;
  }

  @Override
  public int compareTo(Channel o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ChannelId id() {
    return null;
  }

  @Override
  public EventLoop eventLoop() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Channel parent() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelConfig config() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isOpen() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isRegistered() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isActive() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ChannelMetadata metadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SocketAddress localAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SocketAddress remoteAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture closeFuture() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWritable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long bytesBeforeUnwritable() {
    return 0;
  }

  @Override
  public long bytesBeforeWritable() {
    return 0;
  }

  @Override
  public Unsafe unsafe() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelPipeline pipeline() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ByteBufAllocator alloc() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelPromise newPromise() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelProgressivePromise newProgressivePromise() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture newSucceededFuture() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture newFailedFuture(Throwable cause) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelPromise voidPromise() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
      ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture disconnect() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture disconnect(ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture close() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture close(ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture deregister() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture deregister(ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Channel read() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture write(Object msg) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture write(Object msg, ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Channel flush() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg) {
    replyList.add((ByteBuf) msg);
    return null;
  }

}
