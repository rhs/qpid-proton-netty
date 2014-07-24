/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.netty;

import java.nio.ByteBuffer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;

import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Transport;

import org.apache.qpid.proton.demo.AbstractEventHandler;
import org.apache.qpid.proton.demo.Events;
import org.apache.qpid.proton.demo.EventHandler;
import org.apache.qpid.proton.demo.FlowController;
import org.apache.qpid.proton.demo.Handshaker;

public class ProtonNettyHandler extends ChannelInboundHandlerAdapter {

    private Transport transport;
    private Connection connection;
    private Collector collector;

    private Object lock = new Object();
    private EventHandler[] handlers = {new Handshaker(), new FlowController(1024),
                                       new NettyWriter(), new DeliverySink(),
                                       new DeliverySource()};

    public ProtonNettyHandler() {}

    private class NettyWriter extends AbstractEventHandler {
        @Override
        public void onTransport(Transport transport) {
            ChannelHandlerContext ctx = (ChannelHandlerContext) transport.getContext();
            write(ctx);
            int capacity = transport.capacity();
            if (capacity > 0) {
                ctx.read();
            }
        }
    }

    private int sent = 0;
    private int settled = 0;

    private class DeliverySink extends AbstractEventHandler {
        @Override
        public void onDelivery(Delivery delivery) {
            settled++;
            delivery.settle();
        }
    }

    private class DeliverySource extends AbstractEventHandler {

        private int tag = 0;

        private byte[] nextTag() {
            return String.format("%s", tag++).getBytes();
        }

        @Override
        public void onFlow(Link link) {
            if (link instanceof Sender) {
                Sender snd = (Sender) link;
                while (snd.getCredit() > 0 && snd.getQueued() < 1024) {
                    Delivery dlv = snd.delivery(nextTag());
                    dlv.settle();
                    sent++;
                }
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        synchronized (lock) {
            System.out.println("ACTIVE");
            transport = Transport.Factory.create();
            transport.setContext(ctx);

            Sasl sasl = transport.sasl();
            sasl.setMechanisms("ANONYMOUS");
            sasl.server();
            sasl.done(Sasl.PN_SASL_OK);

            connection = Connection.Factory.create();
            collector = Collector.Factory.create();
            connection.collect(collector);
            transport.bind(connection);

            // XXX: really we should fire both of these off of an
            //      initial transport event
            write(ctx);
            int capacity = transport.capacity();
            if (capacity > 0) {
                ctx.read();
            }
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        synchronized (lock) {
            ByteBuf buf = (ByteBuf) msg;
            try {
                while (buf.readableBytes() > 0) {
                    int capacity = transport.capacity();
                    if (capacity <= 0) {
                        throw new IllegalStateException("discarding bytes: " + buf.readableBytes());
                    }
                    ByteBuffer tail = transport.tail();
                    int min = Math.min(capacity, buf.readableBytes());
                    tail.limit(tail.position() + min);
                    buf.readBytes(tail);
                    transport.process();
                    dispatch();
                }
            } finally {
                buf.release();
            }

            int capacity = transport.capacity();
            if (capacity > 0) {
                ctx.read();
            }
        }
    }


    private boolean dispatching = false;

    private void dispatch() {
        synchronized (lock) {
            if (dispatching) {
                return;
            }

            dispatching = true;
            Event ev;
            while ((ev = collector.peek()) != null) {
                for (EventHandler h : handlers) {
                    Events.dispatch(ev, h);
                }
                collector.pop();
            }
            dispatching = false;
        }
    }


    private int offset = 0;

    private void write(final ChannelHandlerContext ctx) {
        synchronized (lock) {
            while (true) {
                int pending = transport.pending();
                if (pending > 0) {
                    final int size = pending - offset;
                    if (size > 0) {
                        ByteBuf buffer = Unpooled.buffer(size);
                        ByteBuffer head = transport.head();
                        head.position(offset);
                        buffer.writeBytes(head);
                        ChannelFuture chf = ctx.writeAndFlush(buffer);
                        offset += size;
                        chf.addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture chf) {
                                if (chf.isSuccess()) {
                                    synchronized (lock) {
                                        transport.pop(size);
                                        offset -= size;
                                    }
                                    write(ctx);
                                    dispatch();
                                } else {
                                    // ???
                                }
                            }
                        });
                    } else {
                        return;
                    }
                } else {
                    if (pending < 0) {
                        closeOnFlush(ctx.channel());
                    }
                    return;
                }
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        synchronized (lock) {
            System.out.println(String.format("CHANNEL CLOSED: settled %s, sent %s", settled, sent));
            transport.close_tail();
            dispatch();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
