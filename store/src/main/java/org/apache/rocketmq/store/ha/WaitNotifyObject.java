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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;

//等待通知对象
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //保存 线程 和是否以通知的标识
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable =
        new HashMap<Long, Boolean>(16);

    protected volatile boolean hasNotified = false;

    //在某些线程调用 wait方法后需要新的线程来调用这个方法 唤醒之前沉睡的线程
    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                //唤醒其他等待的线程
                this.notify();
            }
        }
    }

    //进入的线程进入等待状态 这里需要是 还没被通知过的 也就是调用wakeup 方法后 直接调用这个是不能让线程沉睡的
    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                //钩子
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;

            for (Boolean value : this.waitingThreadTable.values()) {
                //只要 value 为 false 这个值一定是 true 也就是 只要一个线程未被通知 就 调用waitall
                needNotify = needNotify || !value;
                value = true;
            }

            //进入的线程唤醒其他沉睡的线程
            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            //进入这里的线程 又不一定是 上面的线程？？？  通过线程id 来标识 哪条线程调用过 wait
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            //设置成未通知
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            //未通知的 去等待了
            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    //将当前线程 从容器中移除
    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
