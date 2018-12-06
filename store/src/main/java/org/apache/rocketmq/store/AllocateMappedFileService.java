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

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * Create MappedFile in advance
 */
//分配映射文件的 服务
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //最长等待时间为5000
    private static int waitTimeOut = 1000 * 5;
    //key 是什么??? value是分配的请求对象
    private ConcurrentMap<String, AllocateRequest> requestTable =
        new ConcurrentHashMap<String, AllocateRequest>();
    //根据优先队列来 获取分配请求对象
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
        new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;
    //储存动作都是委托给这个对象
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    //根据请求参数 返回映射文件
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        //可以提交的请求数 为2
        int canSubmitRequests = 2;
        //是否 支持瞬间刷盘
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            //如果messageconfig的broker 不是slave 且支持快速失败 (如果在池中没有buffer 可以用)  换句话如果是slave 即使没buffer 也不快速失败
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                //将提交数修改成 剩余buffer 减去队列数 代表一个队列占用一个 buffer
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size();
            }
        }

        //如果 不支持就不修改吗??? 什么意思  可能一般的模式 就是只能一次处理2条  但是是瞬间刷盘时 只要支持快速失败就可以将请求数 更改成 可用buffer数

        //创建分配对象
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        //根据文件路径 和 请求对象 设置到容器中
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        //如果 存放成功
        if (nextPutOK) {
            //如果提交请求数 不够 就移除这个额请求
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            //将请求 存入优先队列  通过比较文件大小 判断优先级
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            //一旦填充到 优先队列 就可以将数量－1   这时如果 还有怎么处理
            canSubmitRequests--;
        }

        //创建第二个请求对象
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        //将请求 保存到容器中
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
                //这里 没有减少提交数？？？
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                //使用栅栏
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    //等待 结束后将请求移除 并获取映射文件 也就是 阻塞期间 应该获得了 映射文件  这个文件是 怎么获取的 是通过spi吗？？？
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    //关闭
    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            //等待该线程执行指定时间
            this.thread.join(this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                //关闭 channel 删除文件
                req.mappedFile.destroy(1000);
            }
        }
    }

    //这个run 是什么时候执行
    public void run() {
        log.info(this.getServiceName() + " service started");

        //不断从优先队列中分配 映射文件
        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Only interrupted by the external thread, will return false
     */
    //run 后 会不断执行该方法
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            //从优先队列中获取任务
            req = this.requestQueue.take();
            //这2个 返回的应该是一个对象吧...  如果对象不存在了 代表这个请求超时了
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }
            //对象不等 也是直接返回
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            //还没有生成 映射文件对象
            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                //能否使用瞬时保存池
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        //从spi 中选择该接口的实现类
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        //初始化 从 瞬时保存池中 拿出一个directBuffer对象
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        //这个也是init
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //这个也是init
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                //计算时间差
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                //文件大小 超过了提交尺寸 应该是不行的
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMapedFileSizeCommitLog()
                    &&
                    this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    //这里好像是 强制写入 什么东西
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                //设置生成的映射文件
                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                //让请求 重新回到优先队列准备下次
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                //解除栅栏  栅栏解除了是不能重新设置的啊 也就是 一次失效 之后调用await 应该是没用的
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    //分配请求对象
    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        //请求分配的 文件路径???
        private String filePath;
        //文件大小
        private int fileSize;
        //创建信号量对象
        private CountDownLatch countDownLatch = new CountDownLatch(1);

        //映射文件对象  这个文件是什么时候被设置的  应该是 给这个请求设置结果的时候
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
