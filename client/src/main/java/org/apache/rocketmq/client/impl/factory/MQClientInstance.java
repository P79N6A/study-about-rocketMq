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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

//mq 的client实例 里面维护了一个 clientconfig 也就是client的抽象 以及一些配置
//每次 启动producer的时候 从manager中获取一个clientinstance 并启动
//一个client可能作为多个生产者 消费者 因为他们都能发送请求
public class MQClientInstance {
    //超时时间
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final InternalLogger log = ClientLogger.getLog();
    //这个就是一个clientconfig
    private final ClientConfig clientConfig;
    //每个instance在创建的 时候都会 生成一个 唯一index 代表是第几个实例
    private final int instanceIndex;
    //client 唯一标示 就是clientconfig 生成的
    private final String clientId;
    //启动的 时间戳
    private final long bootTimestamp = System.currentTimeMillis();
    //这个client 下 管辖的 所有生产者消费者 管理者
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    //这里就是 netty通信用到的 config 这个client 下的 producer consumer 通信都共用这套配置
    private final NettyClientConfig nettyClientConfig;
    //api 实现  那应该也是通过委托 给这个类 完成任务 这个类 处理所有通信的 过程
    private final MQClientAPIImpl mQClientAPIImpl;
    //管理者实例  处理与nameserver的交互 这个和上面的MQAdminExtInner 不同
    private final MQAdminImpl mQAdminImpl;
    //每个 topic 以及 topic路由信息
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    //针对nameserver的操作应该是 线程安全的
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();
    //2级容器 保存了 broker 以及下面的  master slave
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<String, HashMap<Long, String>>();
    //保存版本的 在发送心跳检测的时候 会校验这个值 版本应该就是 broker 的配置信息
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<String, HashMap<String, Integer>>();
    //定时器对象产生的线程 有独有的名字  这样标记应该是便于追踪线程信息
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });
    //请求处理对象
    private final ClientRemotingProcessor clientRemotingProcessor;
    //拉取消息的running 对象 内部有一个定时任务  实际执行的对象是 consumer
    private final PullMessageService pullMessageService;
    //均衡负载  通过自旋 加固定时间等待 完成定时
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    //消费者属性管理器 内部有定时器 定时获取属性新状态 修改应该是在别处
    private final ConsumerStatsManager consumerStatsManager;
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    //套接字对象
    private DatagramSocket datagramSocket;
    private Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
            this.instanceIndex,
            this.clientId,
            this.clientConfig,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    //数据映射  这里将 brokerData  broker地址拦截器 queueData 映射成了mq 对象
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        //首先判断 这个nsv 是否开启 按指定顺序使用mq发送 如果是就会在获取 路由信息的时候设置这个 OrderTopicConfig
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                //num 是 mq 的id
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    //将顺序消息解析后 按照 mqId 个数创建对应的 mq对象
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            //代表路由信息 按照指定顺序 使用mq
            info.setOrderTopic(true);
        } else {
            //默认情况下 对mq 没有顺序要求
            //QueueData 代表针对某个 broker 下 有多少读写队列 权限 访问方式
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                //是否具备写权限  不具备写权限就不创建 发布Info数据体了 因为不能写就不能发布
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    //获取 每个brokerData 对象 代表针对这个topic 能发送到哪些broker下
                    for (BrokerData bd : route.getBrokerDatas()) {
                        //找到 这个queueData 对应的  broker下
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    //找不到 对应的 broker 就放弃这个queueData
                    if (null == brokerData) {
                        continue;
                    }

                    //如果 这个broker集群信息中 不包含master 这个broker 就不考虑 因为 slave 只有读权限 master 才有写权限
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    //根据 这个queueData 的 可写数量创建 等同的 mq 对象 并设置id
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    //通过 路由信息创建对应的  mq 信息
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            //根据 可读队列 来设置 mq  就是说broker 上的 queueData 的 readNum 决定了 这个broker 上有几条用于读取数据的mq
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    //client 启动 当producer 启动的时候可以选择启动client 一般都是默认启动
    public void start() throws MQClientException {

        //这里不用同步块的 话枚举的 改变就不是原子的了
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    // 如果没有在配置文件中找到nsv 的地址 就去 获取 nsv 的地址
                    // 这里是 启动 所以第一次会获取 地址信息并保存到nettyclient对象 关于 nsv 的负载应该是在那里做
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    //启动通信客户端 就是启动nettyclient
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    //开启相关定时任务
                    this.startScheduledTask();
                    // Start pull service
                    //开启拉消息到的服务  就是不断从阻塞队列中获取拉消息的请求头 请求头是在 rebalance 中设置的
                    this.pullMessageService.start();
                    // Start rebalance service
                    //开启 负载服务器  执行的是 rebalance  委托给 consumer
                    this.rebalanceService.start();
                    // Start push service
                    //开启 impl 如果已经启动的情况下 是不操作的 状态是running
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    //client实例 的定时任务
    private void startScheduledTask() {
        //定时获取 nameserver  保证有效性
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    ///client 下的生产者和消费者 订阅和 发布的 topic 信息 的路由信息 需要定时更新
                    //1.从生产者 消费者的订阅发布消息中抽取出所有需要更新的 topic
                    //2.去nsv 上 用topic 获取路由信息
                    //3.将路由信息创建对应的mq对象 并更新本地缓存 mq 对象代表着 能从哪个broker拿/发数据
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    //定时清理下线到的  broker 就是跟本地的 路由信息做同步
                    MQClientInstance.this.cleanOfflineBroker();
                    //定时发送心跳检测 以及更新拦截器对象 到设置的 拦截器地址中
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    //定时将 消费者 的 mq 偏移量 持久化
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    //调整线程池数量???
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    //更新topic 下的 信息
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                //遍历每个消费者对象
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    //每个消费者都订阅 获取 一组订阅消息
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            //设置  topic 队列
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    //获取生产者 发布的 topic 队列  就是从生产者内部的 table 中取出数据
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            //每个topic 都调用这个方法
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * Remove offline broker
     */
    //清理无效的broker
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    //这是存放 broker 集群信息的 容器 哪个broker集群下 的 每个brokerId 和对应address
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        //存放了所有的 brokerId 与地址的对应关系
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            //因为 路由信息的容器是短期更新的 这个broker 地址 存在滞后性 这里就是对比在路由信息中能否找到对应
                            //broker 找不到 就代表这个broker 过期 并不是通过访问nsv 实现
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        //如果 下面的 broker 集群都 失效了 就移除这个broker 集群
                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            //更新 broker
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    //更新容器
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    //检查 broker???
    public void checkClientInBroker() throws MQClientException {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                //非标签模式下  也就是 sql模式下 这里 获取了topic 下的broker 的 master地址
                //这里是随机获取的 有可能 多个topic下 获取了一样的broker吧 ？？？
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                            addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    //发送心跳检测
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                //发送心跳检测给 broker
                this.sendHeartbeatToAllBroker();
                //如果存在 类拦截器 就更新  同样的如果消费者第一次启动就是 类拦截器模式 那么一开始就会创建
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }

    //持久化 消费者偏移量
    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            //委托给消费者实现
            impl.persistConsumerOffset();
        }
    }

    //调整线程池数量  也是委托给 消费者
    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    //更新topic的路由信息
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    //在 topic中是否存在这个地址
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    //发送心跳检测
    private void sendHeartbeatToAllBroker() {
        //生成心跳检测的数据包  就是将本client 的生产者和消费者信息 添加到 数据包中
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }

        //发送给 broker
        if (!this.brokerAddrTable.isEmpty()) {
            //记录 发送心跳检测的总次数
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, HashMap<Long, String>> entry = it.next();
                String brokerName = entry.getKey();
                HashMap<Long, String> oneTable = entry.getValue();
                if (oneTable != null) {
                    for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                        Long id = entry1.getKey();
                        String addr = entry1.getValue();
                        if (addr != null) {
                            //当只有生产者的时候  只往broker master上发送数据 因为 只有master 能写入 所以对于生产者来说 检测的就是master
                            if (consumerEmpty) {
                                if (id != MixAll.MASTER_ID)
                                    continue;
                            }

                            try {
                                //将 被 client的 生产者消费者信息 发送到指定的broker地址上
                                int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                                //这个容器是延迟初始化的 所以第一次都是 put
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                }
                                this.brokerVersionTable.get(brokerName).put(addr, version);
                                //每20次 打印一次日志
                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                //检测该地址是否还有效
                                if (this.isBrokerInNameServer(addr)) {
                                    log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr);
                                } else {
                                    log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                        id, addr);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    //创建类拦截器对象
    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            //如果是 push 类型的消费者  也就是 针对被动获取的数据 是有拦截功能的
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                //获取订阅消息
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    //存在拦截对象
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            //将拦截类信息发送到 filterServer
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    //更新 在指定的 topic的 路由信息  isDefault默认是false  producer 默认是null
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
        DefaultMQProducer defaultMQProducer) {
        try {
            //给 针对nameserver的操作 设置锁 保证nameserver的 同步性
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    //这里代表 传入的 topic找不到数据 所以要使用默认的 topic 查找并且还传入了一个 producer
                    if (isDefault && defaultMQProducer != null) {
                        //用给与的 key 作为 topic  调用客户端 api 访问 作为服务器的 nsv 获取到最新的路由信息
                        //这个 topicKey 是固定的一般 作为 测试使用的topic
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                            1000 * 3);
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                //本地缓存的 路由信息 收到本地 配置影响
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        //这里使用的是指定的 topic 查找
                        //start 的定时任务是走这里 就是委托给 nettyclient
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }
                    //获取到路由消息后  获取的 逻辑是先通过 topic 获取 queuedata 从queueData 的 brokerName 获取brokerData
                    //在获取broker 的 拦截信息 都是从 缓存中获取的 没有再进行额外通信了
                    if (topicRouteData != null) {
                        //获取旧的 路由信息
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //比较新老数据有没有变化
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            //对于 生产者 如果数据为空 就是需要更新 对于消费者 不存在对应topic 信息就是需要更新
                            //true 代表需要更新
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            //提示 数据发生变化
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        //更新的 逻辑
                        if (changed) {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                //当初始化 获取 指定topic 下的路由信息时 这里就将 可以路由到的broker 信息保存在本地
                                //之后 发送消息 或者是 清理失效broker信息 都是从这个容器中获取
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                //数据映射
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //更新生产者的 路由信息 同时这样就能保证可以 发送对应topic 下的 信息了
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        //更新消费者的 信息
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            //更新信息
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    //生成心跳检测的数据包
    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        //使用生产者 和消费者的 数据生成 心跳检测包
        // Consumer
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    //检车 broker 的地址 是否在nameserver 中还有效
    private boolean isBrokerInNameServer(final String brokerAddr) {
        //因为这个数据会随时更新 所以可以看做是最新的 当然还是会出现意外
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    //针对所有broker 创建拦截 对象
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
        final String topic,
        final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            //这个 就是 类的信息
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                fullClassName,
                RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
            && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            //针对 broker 下 的一组 拦截器地址
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        //针对拦截器地址 注册拦截器对象
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                            5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                            topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                consumerGroup, topic, fullClassName);
        }
    }

    //判断数据是否变化
    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    //判断 指定topic 下的数据 是否 需要变化
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        //还是 遍历每个 生产者消费者 判断 信息是否需要变化
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    //就是是否为空  如果 topic 下的 数据为空就需要更新
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    //生产者 发布的消息 就对应 消费者的订阅消息
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    //关闭client 实例  只有所有的 producer consumer 等 全部都关闭了 才能关闭掉这个client
    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    //关闭默认的producer 同样如果已经被关闭了 是不做处理的
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    //与start 对应 关闭相应的服务器
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }
                    //从整个管理中 移除掉实例
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    //注销某些生产者/消费者
    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            //这个同时 不发送心跳包 是什么意思???
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    //注销 某些生产者/消费者
    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            //给 每个broker 地址 都发送 注销的 操作 这里也是委托
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    //注册管理信息
    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    //唤醒 负载对象 更新本地的mq对象
    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        //开始进行 负载的拉取消息 以组为单位
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    //也是委托给消费者处理
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    //根据broker 获取下面随意 一个对象的信息 可能是 master slave
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                //这里其实 只获取第一个地址
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    //通过broker 名字获取地址 默认是 获取master 每个 brokername 下有一个 broker 集群  这里只返回master的
    //这里 确定发送消息时的 broker master地址 因为slave 是不能写入数据的
    public String findBrokerAddressInPublish(final String brokerName) {
        //2级容器
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    //根据给定的id查询 broker 的地址 查不到就返回另一个 同集群下的
    public FindBrokerResult findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        return 0;
    }

    //根据topic 和 group 找到 消费者
    public List<String> findConsumerIdList(final String topic, final String group) {
        //获取 能够订阅 该topic 消息的 随机的broker
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    //根据 topic 返回 broker 地址  一个topic 下有多个 broker 集群 这里是随机获取一个 然后再获取 master地址
    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    //重置指定group下消费者的偏移量
    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            //先定位到 哪一个消费者
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            //暂停
            consumer.suspend();

            //获取 队列和 队列处理对象的关联容器
            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                //将匹配的元素 丢弃  可能是重置的话之前的就不用了
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    //这里是 丢弃消息
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        //更新偏移量 移除不需要的队列
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                //恢复
                consumer.resume();
            }
        }
    }

    //获取消费者状态
    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            //这个偏移量的概念还不懂 委托给消费者
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
        final String consumerGroup,
        final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            //也是委托
            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    //设置一些运行时信息并返回
    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }
}
