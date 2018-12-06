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
package org.apache.rocketmq.common.sysflag;

//消息系统标记
public class MessageSysFlag {
    //压缩
    public final static int COMPRESSED_FLAG = 0x1;
    //这里 代表 有多个标示 那么获取每个标示的 位运算就会更复杂
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;
    //非事务
    public final static int TRANSACTION_NOT_TYPE = 0;
    //准备 提交 回滚
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    //这些 位运算 先 不管 反正就是设置标示
    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }
}
