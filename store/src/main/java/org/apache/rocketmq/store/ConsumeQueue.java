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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

//消费者 队列对象
//映射文件中 每20作为一个单位长度 记录了 偏移量 size 大小 然而数据并不是写在映射文件中 偏移量记录的是一个 实际文件 并通过映射文件中的 fileChannel 对象访问
//也就是 遍历 映射文件 可以获取到 具体在 实际文件的哪个偏移量写入多少大小的数据
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //应该是消费者 队列单元信息的 请求头 大小也是20
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    //关联的消息 储存系统
    private final DefaultMessageStore defaultMessageStore;

    //映射文件队列
    private final MappedFileQueue mappedFileQueue;
    //该队列的主题
    private final String topic;
    //每个队列 对应一个队列id  那么 消费者 应该是多个 消费队列 这个id 的值是怎么确定的???
    private final int queueId;
    //临时存储偏移量的容器  大小 就是偏移量信息的单元长度
    private final ByteBuffer byteBufferIndex;

    //储存的 路径
    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;
    private volatile long minLogicOffset = 0;
    //消费队列的 拓展信息
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        //是否需要创建 mq的 额外信息
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    //使用 mappedfile 加载制定目录下的映射文件信息
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        //存在 额外信息
        if (isExtReadEnable()) {
            //里面也就是 映射文件加载
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    //当重启时 校验数据是否异常 并修正
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            //最后3个文件
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            //映射文件的 大小
            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            //获取该映射文件的 bytebuffer分片
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //获取 该映射文件的 起始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                //每次 读取 单元长的 数据进行校验
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    //这应该是这个文件的格式
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        //每次 移动一个单位长度
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        //一边读取 一边设置物理偏移量
                        this.maxPhysicOffset = offset;
                        //地址 小于给定的值
                        if (isExtAddr(tagsCode)) {
                            //修改地址
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        //偏移量记录的 是 commitLog 的  如果为负数 肯定是异常
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        //这里跳出的是 内层循环  这里很有可能就是以一个映射文件为单位 并且读到了这个文件的末尾
                        break;
                    }
                }

                //如果 当前整个映射文件是写满的
                if (mappedFileOffset == mappedFileSizeLogics) {
                    //将映射文件数据下标移动
                    index++;
                    //不能超过数组长度
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        //这里是跳出外层循环
                        break;
                    } else {
                        //获取 下一个映射文件
                        mappedFile = mappedFiles.get(index);
                        //获取下个文件的 bytebuffer 就直接影响到了上面那个 内循环
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    //代表文件没有写满 也就是 读到末尾了
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            //总结上面的部分 就是每次 以单元长度 为大小 不断读取映射文件 如果 读满一个 就切换到下一个映射文件 如果没有数据可读了 就结束

            //最后 扫描到的 映射文件下标 代表能读取到哪里
            processOffset += mappedFileOffset;
            //从这里开始 刷盘  在映射文件中能拿到 代表是已经刷盘完成了 刷盘才是往mapperbytebuffer当中写东西
            this.mappedFileQueue.setFlushedWhere(processOffset);
            //提交的也是从这里开始
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //清除这个 偏移量之后的 映射文件  在这个偏移量之前的文件保持不变  如果扫描到最后一个文件 就不产生影响 如果 有映射文件没有写满 那么 这个文件后面的映射文件会被销毁
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                //如果 存在 队列额外数据 也调用它的 恢复方法 就是 查看每个映射文件 并去除脏数据
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                //删除这个地址后的映射文件
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    //根据 时间戳 获取队列中的 偏移量
    public long getOffsetInQueueByTime(final long timestamp) {
        //这里是 找出映射文件队列中 第一个修改时间在指定时间戳在之后的文件
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            //获取 文件的起始偏移量  距离最小逻辑偏移量的值
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            //初始化一堆成员属性
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            //获取最小的物理偏移量
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            //获取 信息 抽象成 这个result对象
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                //这个 应该是实际数据 就是去除头部分的数据
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    //也就是到 逻辑偏移量为止
                    while (high >= low) {
                        //应该是 进行二分查找 然后获取到storestamp 与传入参数一样的数据
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        //位置设置成 最小逻辑偏移量到 能到达的最大偏移量的中间值
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            //前进了指定的 单位长度
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        //先不看了 大概就是 获取 在一个 映射文件中的 第几个位置 因为 一个映射文件 每存储一段数据 有一个固定的单位长度
                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    //丢弃 参数 后面的 数据
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        //那么最大 只能到这里了
        this.maxPhysicOffset = phyOffet - 1;
        long maxExtAddr = 1;
        while (true) {
            //获取 最后一个映射文件
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                //重置指针
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                //每次 前进单位长度 读取数据
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        //偏移量 超过 指定值 就要删除  然后会获取前一个映射文件继续执行
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            //前进
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        //是否有数据读到  这里 i 不是0了 已经前进过 如果没有数据这个映射文件也不会被删除
                        if (offset >= 0 && size > 0) {

                            //这里不再往下读能保证 指针的设置不会超过 phyoffset
                            if (offset >= phyOffet) {
                                return;
                            }

                            //获取下一次的数据 同时 更新现有数据
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            //读到末尾就不用读了
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            //再来一次 将给定地址后的数据删除
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    //获取 最后的偏移量
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        //获取 最后一个映射文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            //将位置 向前偏移了一格  这里是 至少能保存他能读取到当前位置 的 offset 和size  如果我写到一半呢??? 这里能够保证我 刚好是写完了 数据然后回退一个单位的第一个long 还是offset吗
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //将位置 标记到前一个 部分
            byteBuffer.position(position);
            //从新的 偏移量开始 读取
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    //刷盘 基本就是委托
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    //删除小于给定偏移量的 数据
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        //更正最小偏移量
        this.correctMinOffset(offset);
        return cnt;
    }

    //更正最小偏移量
    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    //这个 size 是 能读的大小
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        //偏移量 大于 给定的值 应该是正常情况
                        if (offsetPy >= phyMinOffset) {
                            //最小逻辑偏移量 就更改
                            this.minLogicOffset = result.getMappedFile().getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            //地址 也修改成最新的
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            //将 该地址前的数据清除  删除的数据 应该在之前已经被删除掉了 很多 地方 都执行2次 也不清楚原因
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    //这个 偏移值 是针对 映射文件 中的 第几段单元数据
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    //将commitLog 的 消息保存到这里 这样拉取消息的时候 通过topic就能定位到 consumerQueue然后查询就快很多
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        //最大重试次数
        final int maxRetries = 30;
        //是否启用cq 那么拉取消息的时候 应该也要做个判断  能否从cq 上拉取消息
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            //获取  tagsCode
            long tagsCode = request.getTagsCode();
            //是否需要写入 ext数据
            if (isExtWriteEnable()) {
                //创建  单元数据对象  那应该就是一次写一个单元的大小
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                //将请求的数据 设置进去
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                //返回 写入到 映射文件后的终点偏移量
                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            //先校验 然后设置 头部数据就是 那个 20长的数据  也就是每写入一段数据 就写一个 偏移量数据
            //当消息 是 回滚 或者是准备消息时  consumerQueueoffset 是0
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                //更新时间戳
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        //超过重复次数 提示失败
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    //将commitLog 的 某个消息对应的偏移量 保存到这里  当cqOffset 是0时 代表 该消息是 回滚或 准备消息
    //offset 就是 该topic 下这个单元信息记录的 对于commitLog 上的偏移量
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        //这个应该是 当前已经写入的最大偏移量 如果 小代表 之前已经写好了
        if (offset <= this.maxPhysicOffset) {
            return true;
        }

        //重置指针
        this.byteBufferIndex.flip();
        //这个 容器 也是类似于临时容器 只保存一个单位的大小
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        //根据 cqoffset 来估计需要分配几次  回滚 和准备消息 在这里计算就是0  第一次就是20
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        //在不够的情况下 会创建新的映射文件  如果创建新文件起点偏移量 就是传入的值  就是mappedFile.getFileFromOffset()
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            //cq为0的情况下 不处理
            //这是 第一个 文件 的第一次写入
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {

                //一般就是20  代表第一次写入的数据 也就是第一个单位大小 的长度  代表这个 consumerQueue 作为 内部有数据的queue的最小偏移量  0就代表和这个consumerQueue 是无效的
                this.minLogicOffset = expectLogicOffset;
                //预期从20 开始 commit 或 flush的
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                //后面原来那个0 填满
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            //cqOffset 代表 不用加入 也就是 回滚消息 和 事务准备消息
            if (cqOffset != 0) {
                //获得 当前偏移量  这个就是一般情况 已经写入过的映射文件
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                //代表 数据比预期的要多就是重复了
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                //这里就是 > 应该是出错了
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            //将 记录的 对应commitlog的偏移量更新
            //应该会有哪个地方 扩充这个偏移量
            this.maxPhysicOffset = offset;
            //相当的情况下 将请求头写入
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    //填满 每个 空白
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        //分配一个单元的大小
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        //一共有几个单元
        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            //重复添加多少次
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    //在消费者队列中通过一个偏移量定位到 队列中某个 映射文件的位置
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    //将 参数转换 并获取 对应的映射文件数据  然后后设置到 cq对象中 返回
    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        //一个映射文件总共有几个单元数据
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        //后面这段应该是获取到某个映射文件后的 第几个 单元数据 先不管
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    //队列额外信息不为null
    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
