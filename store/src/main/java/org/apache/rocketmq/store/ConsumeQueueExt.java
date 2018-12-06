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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Extend of consume queue, to store something not important,
 * such as message store time, filter bit map and etc.
 * <p/>
 * <li>1. This class is used only by {@link ConsumeQueue}</li>
 * <li>2. And is week reliable.</li>
 * <li>3. Be careful, address returned is always less than 0.</li>
 * <li>4. Pls keep this file small.</li>
 */
//消费者队列的额外信息
public class ConsumeQueueExt {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //映射文件队列
    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;

    private final String storePath;
    private final int mappedFileSize;
    //临时 容器 基本数据都和 消费队列一致
    private ByteBuffer tempContainer;

    public static final int END_BLANK_DATA_LENGTH = 4;

    /**
     * Addr can not exceed this value.For compatible.
     */
    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

    /**
     * Constructor.
     *
     * @param topic topic
     * @param queueId id of queue
     * @param storePath root dir of files to store.
     * @param mappedFileSize file size
     * @param bitMapLength bit map length.
     */
    public ConsumeQueueExt(final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final int bitMapLength) {

        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        //第三个 参数不设置不影响 只是 映射文件不是通过 该服务创建 而是直接创建
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        //这个参数 是创建临时容器的
        if (bitMapLength > 0) {
            this.tempContainer = ByteBuffer.allocate(
                bitMapLength / Byte.SIZE
            );
        }
    }

    /**
     * Check whether {@code address} point to extend file.
     * <p>
     * Just test {@code address} is less than 0.
     * </p>
     */
    public static boolean isExtAddr(final long address) {
        return address <= MAX_ADDR;
    }

    /**
     * Transform {@code address}(decorated by {@link #decorate}) to offset in mapped file.
     * <p>
     * if {@code address} is less than 0, return {@code address} - {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code address}
     * </p>
     */
    //取消装饰   这里可能地址是要经过换算的
    public long unDecorate(final long address) {
        if (isExtAddr(address)) {
            return address - Long.MIN_VALUE;
        }
        return address;
    }

    /**
     * Decorate {@code offset} from mapped file, in order to distinguish with tagsCode(saved in cq originally).
     * <p>
     * if {@code offset} is greater than or equal to 0, then return {@code offset} + {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code offset}
     * </p>
     *
     * @return ext address(value is less than 0)
     */
    //换算地址
    public long decorate(final long offset) {
        if (!isExtAddr(offset)) {
            return offset + Long.MIN_VALUE;
        }
        return offset;
    }

    /**
     * Get data from buffer.
     *
     * @param address less than 0
     */
    //消费者队列的 单元信息
    public CqExtUnit get(final long address) {
        //创建 一个 消费者mq 的单元信息
        CqExtUnit cqExtUnit = new CqExtUnit();
        //通过 地址 兑换成偏移量 获取 数据 并设置到 单元数据中
        if (get(address, cqExtUnit)) {
            return cqExtUnit;
        }

        return null;
    }

    /**
     * Get data from buffer, and set to {@code cqExtUnit}
     *
     * @param address less than 0
     */
    //根据 指定地址 获取到 数据 并设置到单元数据中
    public boolean get(final long address, final CqExtUnit cqExtUnit) {
        //地址不合法
        if (!isExtAddr(address)) {
            return false;
        }

        //将地址兑换成 真正的 偏移量
        final int mappedFileSize = this.mappedFileSize;
        final long realOffset = unDecorate(address);

        //从映射文件列表中 获取对应的映射文件对象
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(realOffset, realOffset == 0);
        if (mappedFile == null) {
            return false;
        }

        //换算成 对应映射文件的 偏移量
        int pos = (int) (realOffset % mappedFileSize);

        //获取数据
        SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos);
        if (bufferResult == null) {
            log.warn("[BUG] Consume queue extend unit({}) is not found!", realOffset);
            return false;
        }
        boolean ret = false;
        try {
            //将 指定数据读取到 单元数据中 结束后释放 对象  也就是说 映射文件 每20长度 后设置一些数据然后不断循环  这20 是 消息头的长度
            ret = cqExtUnit.read(bufferResult.getByteBuffer());
        } finally {
            bufferResult.release();
        }

        return ret;
    }

    /**
     * Save to mapped buffer of file and return address.
     * <p>
     * Be careful, this method is not thread safe.
     * </p>
     *
     * @return success: < 0: fail: >=0
     */
    //取出 单元数据 并设置到映射文件 同时 返回 地址信息
    public long put(final CqExtUnit cqExtUnit) {
        //默认的重试次数
        final int retryTimes = 3;
        try {
            //计算单元数据的真正长度
            int size = cqExtUnit.calcUnitSize();
            //太大了
            if (size > CqExtUnit.MAX_EXT_UNIT_SIZE) {
                log.error("Size of cq ext unit is greater than {}, {}", CqExtUnit.MAX_EXT_UNIT_SIZE, cqExtUnit);
                return 1;
            }
            //当前的 映射文件 如果再写入 这么多数据 会超过最大值
            if (this.mappedFileQueue.getMaxOffset() + size > MAX_REAL_OFFSET) {
                log.warn("Capacity of ext is maximum!{}, {}", this.mappedFileQueue.getMaxOffset(), size);
                return 1;
            }
            // unit size maybe change.but, the same most of the time
            // 那 先将数据储存到临时容器中.
            if (this.tempContainer == null || this.tempContainer.capacity() < size) {
                this.tempContainer = ByteBuffer.allocate(size);
            }

            //根据 重试次数
            for (int i = 0; i < retryTimes; i++) {
                //获取 最后一个映射文件
                MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

                if (mappedFile == null || mappedFile.isFull()) {
                    //这样调用就是 会申请新的 映射文件对象
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }

                if (mappedFile == null) {
                    log.error("Create mapped file when save consume queue extend, {}", cqExtUnit);
                    continue;
                }
                //获取 映射文件的 写指针  如果是 直接 new 映射文件 好像写指针一开始是0  要计算真正的写指针偏移量时 要加上映射文件的起始偏移量
                final int wrotePosition = mappedFile.getWrotePosition();
                //剩余的空间
                final int blankSize = this.mappedFileSize - wrotePosition - END_BLANK_DATA_LENGTH;

                // check whether has enough space.
                // 如果不够写
                if (size > blankSize) {
                    //不够写 就只写入一个-1  也就是在某个地方读到 -1 就代表没有数据了 然后写指针 被移动到末尾 下次就会创建新对象
                    fullFillToEnd(mappedFile, wrotePosition);
                    log.info("No enough space(need:{}, has:{}) of file {}, so fill to end",
                        size, blankSize, mappedFile.getFileName());
                    continue;
                }

                //真正写入数据  写入 的同时 在temp容器中还有一份数据
                if (mappedFile.appendMessage(cqExtUnit.write(this.tempContainer), 0, size)) {
                    //这里 就是将 实际的 写指针的偏移量换算成地址返回
                    return decorate(wrotePosition + mappedFile.getFileFromOffset());
                }
            }
        } catch (Throwable e) {
            log.error("Save consume queue extend error, " + cqExtUnit, e);
        }

        //1 是异常情况
        return 1;
    }

    //填充
    protected void fullFillToEnd(final MappedFile mappedFile, final int wrotePosition) {
        ByteBuffer mappedFileBuffer = mappedFile.sliceByteBuffer();
        mappedFileBuffer.position(wrotePosition);

        // ending.
        //这个映射文件 只设置一个 -1 就将指针移动到 映射文件尾部了
        mappedFileBuffer.putShort((short) -1);

        mappedFile.setWrotePosition(this.mappedFileSize);
    }

    /**
     * Load data from file when startup.
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue extend" + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * Check whether the step size in mapped file queue is correct.
     */
    public void checkSelf() {
        this.mappedFileQueue.checkSelf();
    }

    /**
     * Recover.
     */
    //恢复
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles == null || mappedFiles.isEmpty()) {
            return;
        }

        // load all files, consume queue will truncate extend files.
        int index = 0;

        //这里 不同的是 不是只取最后3个文件 而是全部的文件
        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        //创建 一个 单元信息对象
        CqExtUnit extUnit = new CqExtUnit();
        while (true) {
            //让bytebuffer 跳过 第一个short 储存的 长度  这个值 好像就是size  同时将 extUnit 的 size 设置成这个值
            extUnit.readBySkip(byteBuffer);

            // check whether write sth.
            //这里 又获取 一次size
            if (extUnit.getSize() > 0) {
                //偏移量增加 size  那么就是在 不断的 往下读数据的过程 一旦出现size 为负数代表没数据可读了
                mappedFileOffset += extUnit.getSize();
                continue;
            }

            //没数据可读 代表读完了一个映射文件 因为一个映射文件里就是多次循环并储存上面的数据
            index++;
            //还没有读到最后一个映射文件前
            if (index < mappedFiles.size()) {
                //获取映射文件
                mappedFile = mappedFiles.get(index);
                //下面的 3行是为下次循环 做初始工作
                byteBuffer = mappedFile.sliceByteBuffer();
                //获取起始偏移量
                processOffset = mappedFile.getFileFromOffset();
                //将 读到映射文件的哪部分 清零  指针的 偏移量应该是 processOffset+mappedFileOffset
                mappedFileOffset = 0;
                log.info("Recover next consume queue extend file, " + mappedFile.getFileName());
                continue;
            }

            //这里读完了全部的映射文件
            log.info("All files of consume queue extend has been recovered over, last mapped file "
                + mappedFile.getFileName());
            break;
        }

        //获取最后一个映射文件的偏移量
        processOffset += mappedFileOffset;
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        //将这个偏移量后的 映射文件销毁
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    /**
     * Delete files before {@code minAddress}.
     *
     * @param minAddress less than 0
     */
    //删除  在地址前的数据
    public void truncateByMinAddress(final long minAddress) {
        if (!isExtAddr(minAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by min {}.", minAddress);

        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        //兑换成偏移量
        final long realOffset = unDecorate(minAddress);

        for (MappedFile file : mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;

            //在给定的偏移量前的 全部销毁
            if (fileTailOffset < realOffset) {
                log.info("Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}", file.getFileName(),
                    fileTailOffset, realOffset);
                if (file.destroy(1000)) {
                    willRemoveFiles.add(file);
                }
            }
        }

        //从映射文件中 删除指定的文件
        this.mappedFileQueue.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * Delete files after {@code maxAddress}, and reset wrote/commit/flush position to last file.
     *
     * @param maxAddress less than 0
     */
    //删除这个偏移量前的数据
    public void truncateByMaxAddress(final long maxAddress) {
        if (!isExtAddr(maxAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by max {}.", maxAddress);

        //获取 地址对应的偏移量所在的映射文件的  数据
        CqExtUnit cqExtUnit = get(maxAddress);
        if (cqExtUnit == null) {
            log.error("[BUG] address {} of consume queue extend not found!", maxAddress);
            return;
        }

        //换成偏移量
        final long realOffset = unDecorate(maxAddress);

        //offset就是 这个映射文件的 尾部 作为分界线  以这个文件为起点 后面的映射文件销毁
        this.mappedFileQueue.truncateDirtyFiles(realOffset + cqExtUnit.getSize());
    }

    /**
     * flush buffer to file.
     */
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    /**
     * delete files and directory.
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * Max address(value is less than 0).
     * <p/>
     * <p>
     * Be careful: it's an address just when invoking this method.
     * </p>
     */
    public long getMaxAddress() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return decorate(0);
        }
        //映射文件起始偏移量 加上 写指针 才是真正的写指针偏移量
        return decorate(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
    }

    /**
     * Minus address saved in file.
     */
    public long getMinAddress() {
        MappedFile firstFile = this.mappedFileQueue.getFirstMappedFile();
        if (firstFile == null) {
            return decorate(0);
        }
        return decorate(firstFile.getFileFromOffset());
    }

    /**
     * Store unit.
     */
    public static class CqExtUnit {
        //固定的头部大小
        public static final short MIN_EXT_UNIT_SIZE
            = 2 * 1 // size, 32k max
            + 8 * 2 // msg time + tagCode
            + 2; // bitMapSize

        public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

        public CqExtUnit() {
        }

        public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
            this.tagsCode = tagsCode == null ? 0 : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
        }

        /**
         * unit size
         */
        private short size;
        /**
         * has code of tags
         */
        private long tagsCode;
        /**
         * the time to store into commit log of message
         */
        private long msgStoreTime;
        /**
         * size of bit map
         */
        private short bitMapSize;
        /**
         * filter bit map
         */
        private byte[] filterBitMap;

        /**
         * build unit from buffer from current position.
         */
        private boolean read(final ByteBuffer buffer) {
            //当前位置 不能接近 limit
            if (buffer.position() + 2 > buffer.limit()) {
                return false;
            }

            //获取 bytebuffer第一个元素 且应该是 size
            this.size = buffer.getShort();

            if (this.size < 1) {
                return false;
            }

            this.tagsCode = buffer.getLong();
            this.msgStoreTime = buffer.getLong();
            this.bitMapSize = buffer.getShort();

            //因为 bitmap长度为0 就不需要读取数据了
            if (this.bitMapSize < 1) {
                return true;
            }

            if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
                this.filterBitMap = new byte[bitMapSize];
            }

            //将 数据 读取 这个map大小的长度 到里面
            buffer.get(this.filterBitMap);
            return true;
        }

        /**
         * Only read first 2 byte to get unit size.
         * <p>
         * if size > 0, then skip buffer position with size.
         * </p>
         * <p>
         * if size <= 0, nothing to do.
         * </p>
         */
        //跳过 指定的size
        private void readBySkip(final ByteBuffer buffer) {
            ByteBuffer temp = buffer.slice();

            //获取 分片对象的 长度
            short tempSize = temp.getShort();
            this.size = tempSize;

            //调整指针
            if (tempSize > 0) {
                buffer.position(buffer.position() + this.size);
            }
        }

        /**
         * Transform unit data to byte array.
         * <p/>
         * <li>1. @{code container} can be null, it will be created if null.</li>
         * <li>2. if capacity of @{code container} is less than unit size, it will be created also.</li>
         * <li>3. Pls be sure that size of unit is not greater than {@link #MAX_EXT_UNIT_SIZE}</li>
         */
        //给 参数 写入指定的内容
        private byte[] write(final ByteBuffer container) {
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            //这个 size 是 什么意思??? 加上一个 最小的  单元长度
            //可能一次写入的 就应该是这么大 一次写入一个单元的长度
            //这个单元的长度 应该就是 size+tagscode+ msgstoretime + bitmapsize  2+8+8+2
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

            ByteBuffer temp = container;

            //长度不够重新分配
            if (temp == null || temp.capacity() < this.size) {
                temp = ByteBuffer.allocate(this.size);
            }

            //重置指针
            temp.flip();
            temp.limit(this.size);

            //写入数据
            temp.putShort(this.size);
            temp.putLong(this.tagsCode);
            temp.putLong(this.msgStoreTime);
            temp.putShort(this.bitMapSize);
            if (this.bitMapSize > 0) {
                temp.put(this.filterBitMap);
            }

            //将写入的数据 以byte[] 返回
            return temp.array();
        }

        /**
         * Calculate unit size by current data.
         */
        //当前数据长度 就是 必要的 头部信息 加上真正的实体数据 也就是 filterBitmap
        private int calcUnitSize() {
            int sizeTemp = MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
            return sizeTemp;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(final long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(final long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public byte[] getFilterBitMap() {
            if (this.bitMapSize < 1) {
                return null;
            }
            return filterBitMap;
        }

        //设置拦截对象
        public void setFilterBitMap(final byte[] filterBitMap) {
            this.filterBitMap = filterBitMap;
            // not safe transform, but size will be calculate by #calcUnitSize
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        }

        public short getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CqExtUnit))
                return false;

            CqExtUnit cqExtUnit = (CqExtUnit) o;

            if (bitMapSize != cqExtUnit.bitMapSize)
                return false;
            if (msgStoreTime != cqExtUnit.msgStoreTime)
                return false;
            if (size != cqExtUnit.size)
                return false;
            if (tagsCode != cqExtUnit.tagsCode)
                return false;
            if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) size;
            result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
            result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
            result = 31 * result + (int) bitMapSize;
            result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CqExtUnit{" +
                "size=" + size +
                ", tagsCode=" + tagsCode +
                ", msgStoreTime=" + msgStoreTime +
                ", bitMapSize=" + bitMapSize +
                ", filterBitMap=" + Arrays.toString(filterBitMap) +
                '}';
        }
    }
}
