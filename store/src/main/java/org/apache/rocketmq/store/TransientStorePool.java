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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

//瞬时保存池
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //池大小
    private final int poolSize;
    //文件尺寸
    private final int fileSize;
    //多个 bytebuffer
    private final Deque<ByteBuffer> availableBuffers;
    //消息保存 配置
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMapedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    //可能是创建的时候 这块内存就要锁起来 不让别人动了
    public void init() {
        //池的大小 代表 几个 buffer对象
        for (int i = 0; i < poolSize; i++) {
            //创建直接内存对象
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            //获取内存地址
            final long address = ((DirectBuffer) byteBuffer).address();
            //定位地址偏移量的
            Pointer pointer = new Pointer(address);
            //给物理内存上锁
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            //给队列 添加这个对象
            availableBuffers.offer(byteBuffer);
        }
    }

    //销毁方法
    public void destroy() {
        //为 内存块解锁
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            //这是sum 的对象 先不管
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    //将 buffer 归还到双端队列中  归还的时候 重置标识
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        //设置 limit 指针
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    //从双端队列头部 获取 buffer
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    //首先是 支持瞬间储存池  才能返回双端队列的大小
    public int remainBufferNumbs() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
