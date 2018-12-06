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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

//HA 服务对象  双机集群  同步 主从 broker
//这是 在 broker master上的
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //连接数 也就是 slave的数量
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    //保存连接数的 容器
    private final List<HAConnection> connectionList = new LinkedList<>();

    //处理连接的 服务对象
    private final AcceptSocketService acceptSocketService;

    //消息储存对象
    private final DefaultMessageStore defaultMessageStore;

    //管理沉睡的线程对象
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    //所有slave 中最大的偏移量
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    //检查 同步工作是否正常
    private final GroupTransferService groupTransferService;

    //HA 客户端
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    //委托 客户端对象更换地址
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    //将提交请求 保存到容器中
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    //slave 节点是否可用
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                    //这个值 还不明确是 什么用的 获取到的应该是 当前slave 写入的位置 也就是master写入的 距离当前slave写入的位置
                    //要小于某个值
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    //一旦 偏移量合适 就会拉取数据
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            //当收到 slave 发送的 确认 添加 的  偏移量信息后 就更新 这里的偏移量 并 触发 之后的写入
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    //启动相关应用程序
    public void start() throws Exception {
        //就是 serverchannel 的 注册监听事件
        this.acceptSocketService.beginAccept();
        //启动 轮询 select 并保存 生成的 HAconnection 并启动
        this.acceptSocketService.start();
        //不断确认 同步传输 是否正常
        this.groupTransferService.start();
        //启动 进行 ha的客户端 就是连接到服务器 不断读取 偏移量 并写入到 本地的 commitLog对象中  启动前要先设置master地址
        //一般启动时 会加载配置文件 读取地址
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        //关闭对应服务
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    //接受 slave 创建的 连接对象
    class AcceptSocketService extends ServiceThread {
        //本地地址
        private final SocketAddress socketAddressListen;
        //服务端 channel 这个是nio的channel
        private ServerSocketChannel serverSocketChannel;
        //选择器对象
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        //开始监听 连接事件
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            //允许连接 处理超时状态的 地址和端口 每个连接在断开的时候会有一段时间进入超时状态
            this.serverSocketChannel.socket().setReuseAddress(true);
            //绑定到本地端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            //非阻塞模式
            this.serverSocketChannel.configureBlocking(false);
            //接受连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        //这里应该就是 开启监听连接 并保存连接的过程
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //选择等待时间后 获取选择对象
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        //遍历获取到的选择对象 一旦 包含accept时间 就获取channel对象
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        //通过 接受到的 channel 对象创建 连接对象
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        //监听读/写事件
                                        conn.start();
                                        //维护连接对象
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        //已经处理过的事件 就移除
                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     */
    //检查 主从同步是否正常
    class GroupTransferService extends ServiceThread {

        //管理 线程wait的 对象
        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        //将请求保存到 准备写入的容器中
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            //父类的对象 表示 服务可以启动了 父类一般会调用 waitForRunning 开启栅栏
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        //唤醒沉睡的等待运输的线程
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        //为什么要用2个列表来保存请求
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        //等待数据传输  这里没有进入传输 只是判断 是否 传输完成
        private void doWaitTransfer() {
            //保证读队列不变
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    //同步刷盘对象
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        //slave 的位置超过要写入的位置 代表写入已经完成了
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        //未完成 就沉睡
                        for (int i = 0; !transferOK && i < 5; i++) {
                            //当前线程沉睡
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        //还是未完成 就返回写入超时
                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        //解除 阻塞 并 设置结果 在刷盘的时候应该会 阻塞 保证写入的数据是按顺序的
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        //启动 ha服务器时 会触发这个
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //等待 栅栏被唤醒  应该是由某个 开始传输的 方法
                    this.waitForRunning(10);
                    //唤醒后等待传输结果
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        //执行前后 都会交换读写队列 为什么要分成2个对象 是在获取 read 对象的时候保证能读取到新的write 对象吗
        //如果使用一个对象 读写 就会降低吞吐量  如何保证 这个方法的原子性??? 即使读是最新的 在 交换 的中间过程中还是会出现问题
        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    //HA 客户端对象  代表 这个service 同时具备 客户端和 服务器的职能
    class HAClient extends ServiceThread {
        //设定了 一次读取的大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        //原子变量更新 broker 地址
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        //请求对象的大小
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        //客户端channel
        private SocketChannel socketChannel;
        private Selector selector;
        //上次 写入 slave 的时间
        private long lastWriteTimestamp = System.currentTimeMillis();

        //当前的 写入进度
        private long currentReportedOffset = 0;
        //当前缓存区的指针
        private int dispatchPostion = 0;
        //读缓存区 与 读缓存区备份
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        //更新客户端地址
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        //是否需要进行心跳检测
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            //超过一次心跳检测的间隔时间 才开始发送
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        //将传入的参数 写入到 客户端channel中 通知服务器对象
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            //切换为 读模式
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    //一旦 写入成功 指针会移动到末尾的 所以就是 至多尝试3次  成功就 往下走
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        //重新分配缓冲区的大小
        private void reallocateByteBuffer() {
            //如果只能读取到部分数据 写不下了 就要把这些数据单独提取出来
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            if (remain > 0) {
                //截取部分数据到 backup中
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            //将 backup 和read 的数据交换  这个方法应该是在单线程环境调用的
            this.swapByteBuffer();

            //交换后 这个节点应该就是末尾
            this.byteBufferRead.position(remain);
            //现在能够写入的 空间就是 dispatchPosition - 0
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            //这样相当于 现在 read 就保留了 上次剩下的
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        //处理读事件
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //将channel 的数据读取到bytebuffer中  每次读取position 也会增加
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    //一旦读取到数据 就将次数清零
                    if (readSize > 0) {
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        readSizeZeroTimes = 0;
                        //读取到数据就分发请求  应该是要写入到commitLog中
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                        //3次没读取到数据 退出这个方法
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        //接受master 发来的 数据 并写入到 slave 的 commitlog中
        private boolean dispatchReadRequest() {
            //请求头长度
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            //数据都是读取到 readBytebuffer 对象中的 所以通过操纵这个对象的指针获取数据
            //这个就是终点的 指针 每次 读取数据后 都设置到 终点的位置
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                //dispatchPosition 是分发的起点
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                //这2个的 差值代表读取了多少数据 一次读取的数据最少要大于一个头部信息的长度 否则不处理
                if (diff >= msgHeaderSize) {
                    //从上次的终点开始读取 因为现在的position 是读取的尾节点
                    //返回master的 偏移量 应该是起点的偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    //如果当前是 slave 节点 那么 commitLog 就是要写入的 起点偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        //传过来的起点 和上次的终点 应该是一样的
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    //包含一个完整的数据体  就可以开始取数据了  如果 body 长度是0 这里就相当于不做操作 其实0 就代表本次数据是心跳包
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        //将数据写入到 byte中
                        this.byteBufferRead.get(bodyData);

                        //从起点开始 写入 bodyData 的数据
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        //将 position 复位
                        this.byteBufferRead.position(readSocketPos);
                        //移动到本次处理的末尾
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        //每写入一段数据 就要通知一次 master 失败 就代表出现异常了
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                //因为 结尾将指针移动到 尾部 这里就会为true
                if (!this.byteBufferRead.hasRemaining()) {
                    //重新分配容器
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            //获取当前的偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            //这里是更新这个偏移量指标
            if (currentPhyOffset > this.currentReportedOffset) {
                //这里更新了 当前写入的 偏移量
                this.currentReportedOffset = currentPhyOffset;
                //将偏移量 通知到远程 服务器
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    //失败代表 服务器连接关闭 就是master出问题
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        //重新连接到 master
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                //获取 地址 重新创建连接
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        //连接  这里连接到 master地址 就会触发  创建HaConnect的 事件  然后保存
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            //注册读事件
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                //设置 当前读取到的位置是 commitLog  的最大 偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        //关闭master  节点
        private void closeMaster() {
            //如果 channel 存在 进入这里一般是 服务器出了问题就需要断开连接
            if (null != this.socketChannel) {
                try {

                    //关闭注册事件 以及 channel
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                //重置 成员变量
                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        //HAClient 的 具体逻辑 在 service启动的时候就会触发
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //连接成功后
                    if (this.connectMaster()) {

                        //是否需要进行心跳 检测 当最后写入时间  与当前时间 值超过心跳检测时间就要 进行
                        if (this.isTimeToReportOffset()) {
                            //将当前偏移量通知到 master  这样会 触发 master 往slave写数据
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            //代表连接断开
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);

                        //阻塞一定时间后 应该是要看看 有没有读到数据 没有读取到数据时也是返回 true 只有当 出现异常 如偏移量不正确  或者连接不到master 才会返回false
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        //当断开连接时 这里就会失败 然后下次循环又会开始 创建 连接
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        //这里是 超时的意思
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        //等待指定时候后重试
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
