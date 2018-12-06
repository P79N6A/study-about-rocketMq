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
package org.apache.rocketmq.client.producer;

import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;

//发送事务mq 的 对象
public class TransactionMQProducer extends DefaultMQProducer {
    //检查本地事务状态的接口
    private TransactionCheckListener transactionCheckListener;
    //检查事务的 线程数 为1
    private int checkThreadPoolMinSize = 1;
    private int checkThreadPoolMaxSize = 1;
    //应该是单位时间内接受的请求次数把
    private int checkRequestHoldMax = 2000;

    //任务执行者
    private ExecutorService executorService;

    //这是推荐使用的新的监听器  具有执行本地事务 和 检查本地事务的 功能
    private TransactionListener transactionListener;

    public TransactionMQProducer() {
    }

    public TransactionMQProducer(final String producerGroup) {
        super(producerGroup);
    }

    public TransactionMQProducer(final String producerGroup, RPCHook rpcHook) {
        super(producerGroup, rpcHook);
    }

    @Override
    public void start() throws MQClientException {
        //这里 初始化事务
        this.defaultMQProducerImpl.initTransactionEnv();
        //启动实际上也是 委托 impl
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        //解除 事务环境
        this.defaultMQProducerImpl.destroyTransactionEnv();
    }

    /**
     * This method will be removed in the version 5.0.0, method <code>sendMessageInTransaction(Message,Object)</code>}
     * is recommended.
     */
    @Override
    @Deprecated
    //父类是不支持发送 事务消息的 然后子类来实现  localTransactonExecuter 封装了 执行本地事务
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException {
        //因为新版本中这个 对象不推荐使用了 所以这个方法也不推荐使用了
        if (null == this.transactionCheckListener) {
            throw new MQClientException("localTransactionBranchCheckListener is null", null);
        }

        //委托实现
        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, tranExecuter, arg);
    }

    //发送事务消息的 入口
    @Override
    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final Object arg) throws MQClientException {
        //事务消息 监听器必须存在
        if (null == this.transactionListener) {
            throw new MQClientException("TransactionListener is null", null);
        }

        return this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg);
    }

    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }

    public int getCheckThreadPoolMinSize() {
        return checkThreadPoolMinSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckThreadPoolMinSize(int checkThreadPoolMinSize) {
        this.checkThreadPoolMinSize = checkThreadPoolMinSize;
    }

    public int getCheckThreadPoolMaxSize() {
        return checkThreadPoolMaxSize;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckThreadPoolMaxSize(int checkThreadPoolMaxSize) {
        this.checkThreadPoolMaxSize = checkThreadPoolMaxSize;
    }

    public int getCheckRequestHoldMax() {
        return checkRequestHoldMax;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setCheckRequestHoldMax(int checkRequestHoldMax) {
        this.checkRequestHoldMax = checkRequestHoldMax;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }
}
