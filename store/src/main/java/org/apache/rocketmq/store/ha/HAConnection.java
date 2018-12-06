/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

//master 和 slave 的连接对象
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //关联的 ha服务对象
    private final HAService haService;
    //channel
    private final SocketChannel socketChannel;
    //客户端地址 也即是 slave 地址
    private final String clientAddr;
    //写服务  负责给 channel写入数据  从master 写给 slave
    private WriteSocketService writeSocketService;
    //读服务  负责从 channel读取数据  读取从slave 发来的 已经写入的偏移量
    private ReadSocketService readSocketService;

    //请求 的 拉取偏移量
    private volatile long slaveRequestOffset = -1;
    //从服务端反馈的 拉取的偏移量
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        //这里是master 接收到的 slave 的 地址
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        //这是一些 tcp 相关的设置
        //这里就是 不设置 逗留时间
        this.socketChannel.socket().setSoLinger(false, -1);
        //不延迟
        this.socketChannel.socket().setTcpNoDelay(true);
        //一次 能接受的数据大小  这应该涉及到 拆包和 粘包的概念
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        //每创建一个 连接 就增加一个连接数
        this.haService.getConnectionCount().incrementAndGet();
    }

    //这个 由谁来调用???
    public void start() {
        //启动2个 服务对象
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        //关闭2个 服务对象
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                //关闭 channel
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    //socket 的 读服务  应该是针对 master 接受 slave 返回的 当前读到哪个偏移量的信息
    class ReadSocketService extends ServiceThread {
        //一次 最多读取的 bytebuffer长度
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        //选择器 接受新连接
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private int processPostion = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            //Selector.open 可以直接创建一个选择器对象
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //给 选择器注册读事件  这个channel 就是 slave 的channel
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            //这样就不用自行维护该线程的生命周期了
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //不断选择
                    this.selector.select(1000);
                    //每选择多少时间就看看 从channel 中读取数据到 bytebuffer中 并以8字节为单位 每次解析出偏移量 调用 haService的  notify方法
                    //这里是不断 收到slave 传过来的 ack信息 代表slave 一直有效
                    boolean ok = this.processReadEvent();
                    //只有异常才会返回 false  没有数据 也是返回true
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    //2次 读取间的时间间隔
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    //给与的时间间隔 如果超过了 设置的就停止
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            //上面出现了 异常 就要终止 这里并没有终止什么 程序  只是简单的把stop 设置成true
            this.makeStop();

            //同上
            writeSocketService.makeStop();

            //移除这个连接  也就是从容器中删除这个对象
            haService.removeConnection(HAConnection.this);

            //连接数-1
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            //关闭 选择Key对象
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        //从选择器中获取读事件对象
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            //如果没有剩余空间了 就清空 等下要用这个对象去 读取数据
            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPostion = 0;
            }

            //将指针重置 后 又可以读取了
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //从 channel 获取数据
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    //代表读到了数据
                    if (readSize > 0) {
                        //清空 读到0的 次数
                        readSizeZeroTimes = 0;
                        //获取 当前系统时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        //超过单位长度 就可以读取
                        if ((this.byteBufferRead.position() - this.processPostion) >= 8) {
                            //得到完整的 数据长度  去掉的是 不满单位长度的部分
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            //代表读最后一个 长度为 8的东西 应该就是偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            //标识进度
                            this.processPostion = pos;

                            //当前确认读到的 偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            //需要初始化的话   第一次设置后就不会修改了？？？
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            //通知 master 继续传入 数据
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                        //读取不到数据的时候
                    } else if (readSize == 0) {
                        //连续3次 没有读取到数据 就返回
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        //这个应该是异常情况
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    //将master 的  数据 写给slave
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        //连接对象
        private final SocketChannel socketChannel;
        //请求头大小
        private final int headerSize = 8 + 4;
        //分配指定大小
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        private long nextTransferFromWhere = -1;
        //根据 slave 传来的 偏移量 获取 数据体 并写回给 slave
        private SelectMappedBufferResult selectMappedBufferResult;
        //上次写入是否完成
        private boolean lastWriteOver = true;
        //最后一次写入的时间
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            //这样不需要管理线程的生命周期
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    //没有收到 要求的 偏移量 就不写入数据了  当 haclient start时 第一次会将 它的 commitLog 偏移量发过来 这样就可以开始写数据了
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    //第一次就是 -1
                    if (-1 == this.nextTransferFromWhere) {
                        //代表 slave 还没有写入任何数据
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            //得到整数倍的  映射文件的大小
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMapedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            //从 最后一个映射文件开始
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //从请求的偏移量开始
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    //如果上一次写完了 就创建一个新的头对象 开始写入 失败了 就直接重写
                    if (this.lastWriteOver) {

                        //获取距离上次写完的时间间隔
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        //需要发送心跳检测  也就是不需要body 的  所以这里没有 从commitLog 中拉取数据
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            //创建了 头对象
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            //代表body 长度是0
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            //发送数据
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    //从 给定的偏移量中 获取 映射文件信息
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        //不能超过 上限
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        //将偏移量 移动 保证下次取到的也是合适的数据
                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        //设置请求体
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        //这样才算一次 完整的发送 因为有数据体
                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        //暂停等待请求  可能唤醒的时候 就有数据体了  那还是会写一个空的数据头啊
                        //每个进入的线程都会沉睡 这里有个时间限制 在沉睡一定时间后自动唤醒
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            //循环结束 后 就是 停止了 就 做清理工作
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            //释放物理内存
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            //设置 stop 标识
            this.makeStop();

            readSocketService.makeStop();

            //移除连接对象
            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        //发送数据
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                //将 bytebuffer数据写入到channel中
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            //代表 没有从commitLog 中获取数据 那么只要头部写入成功就是成功了
            if (null == this.selectMappedBufferResult) {
                //write 的时候应该会移动读指针
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            //只有头写完的时候才能写body
            if (!this.byteBufferHeader.hasRemaining()) {
                //代表 有数据体
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    //将数据体写入
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            //数据都写完时 就是成功
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            //都写入完毕就 释放
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
