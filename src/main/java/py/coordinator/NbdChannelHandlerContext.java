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

package py.coordinator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import java.util.Collection;


public class NbdChannelHandlerContext<C extends Object> implements ChannelHandlerContext {

  private Collection<C> results;
  private Channel channel;

  public NbdChannelHandlerContext(Channel channel) {
    this.results = null;
    this.channel = channel;
  }

  public Collection<C> getResult() {
    return results;
  }

  @Override
  public <T> Attribute<T> attr(AttributeKey<T> key) {

    return null;
  }

  @Override
  public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
    return false;
  }

  @Override
  public Channel channel() {
    return channel;
  }

  @Override
  public EventExecutor executor() {

    return null;
  }

  @Override
  public String name() {

    return null;
  }

  @Override
  public ChannelHandler handler() {

    return null;
  }

  @Override
  public boolean isRemoved() {

    return false;
  }

  @Override
  public ChannelHandlerContext fireChannelRegistered() {

    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelUnregistered() {

    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelActive() {

    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelInactive() {

    return null;
  }

  @Override
  public ChannelHandlerContext fireExceptionCaught(Throwable cause) {

    return null;
  }

  @Override
  public ChannelHandlerContext fireUserEventTriggered(Object event) {

    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ChannelHandlerContext fireChannelRead(Object msg) {
    results = (Collection<C>) msg;
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelReadComplete() {

    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelWritabilityChanged() {

    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress) {

    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress) {

    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {

    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
      ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture disconnect() {

    return null;
  }

  @Override
  public ChannelFuture disconnect(ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture close() {

    return null;
  }

  @Override
  public ChannelFuture close(ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture deregister() {

    return null;
  }

  @Override
  public ChannelFuture deregister(ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelHandlerContext read() {

    return null;
  }

  @Override
  public ChannelFuture write(Object msg) {

    return null;
  }

  @Override
  public ChannelFuture write(Object msg, ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelHandlerContext flush() {

    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {

    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg) {

    return null;
  }

  @Override
  public ChannelPipeline pipeline() {

    return null;
  }

  @Override
  public ByteBufAllocator alloc() {

    return null;
  }

  @Override
  public ChannelPromise newPromise() {

    return null;
  }

  @Override
  public ChannelProgressivePromise newProgressivePromise() {

    return null;
  }

  @Override
  public ChannelFuture newSucceededFuture() {

    return null;
  }

  @Override
  public ChannelFuture newFailedFuture(Throwable cause) {

    return null;
  }

  @Override
  public ChannelPromise voidPromise() {

    return null;
  }

}
