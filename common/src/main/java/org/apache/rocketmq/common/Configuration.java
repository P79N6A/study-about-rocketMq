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

package org.apache.rocketmq.common;

import org.apache.rocketmq.logging.InternalLogger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//配置对象
public class Configuration {

    private final InternalLogger log;

    private List<Object> configObjectList = new ArrayList<Object>(4);
    private String storePath;
    private boolean storePathFromConfig = false;
    private Object storePathObject;
    private Field storePathField;
    private DataVersion dataVersion = new DataVersion();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * All properties include configs in object and extend properties.
     */
    private Properties allConfigs = new Properties();

    public Configuration(InternalLogger log) {
        this.log = log;
    }

    public Configuration(InternalLogger log, Object... configObjects) {
        this.log = log;
        if (configObjects == null || configObjects.length == 0) {
            return;
        }
        for (Object configObject : configObjects) {
            //添加指定配置对象
            registerConfig(configObject);
        }
    }

    public Configuration(InternalLogger log, String storePath, Object... configObjects) {
        this(log, configObjects);
        //设置持久化的路径
        this.storePath = storePath;
    }

    /**
     * register config object
     *
     * @return the current Configuration object
     */
    //注册配置 就是添加到容器
    public Configuration registerConfig(Object configObject) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {

                //属性信息
                Properties registerProps = MixAll.object2Properties(configObject);

                //整合属性
                merge(registerProps, this.allConfigs);

                //将配置保存到容器中
                configObjectList.add(configObject);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("registerConfig lock error");
        }
        return this;
    }

    /**
     * register config properties
     *
     * @return the current Configuration object
     */
    //整合属性
    public Configuration registerConfig(Properties extProperties) {
        if (extProperties == null) {
            return this;
        }

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                merge(extProperties, this.allConfigs);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("register lock error. {}" + extProperties);
        }

        return this;
    }

    /**
     * The store path will be gotten from the field of object.
     *
     * @throws java.lang.RuntimeException if the field of object is not exist.
     */
    //通过给定对象的某个 属性来设置 保存地址
    public void setStorePathFromConfig(Object object, String fieldName) {
        assert object != null;

        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                this.storePathFromConfig = true;
                this.storePathObject = object;
                // check
                this.storePathField = object.getClass().getDeclaredField(fieldName);
                assert this.storePathField != null
                    && !Modifier.isStatic(this.storePathField.getModifiers());
                this.storePathField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("setStorePathFromConfig lock error");
        }
    }

    //获取保存的路径
    private String getStorePath() {
        String realStorePath = null;
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                realStorePath = this.storePath;

                //如果是从对象中获得的
                if (this.storePathFromConfig) {
                    try {
                        realStorePath = (String) storePathField.get(this.storePathObject);
                    } catch (IllegalAccessException e) {
                        log.error("getStorePath error, ", e);
                    }
                }
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getStorePath lock error");
        }

        return realStorePath;
    }

    public void setStorePath(final String storePath) {
        this.storePath = storePath;
    }

    //更新属性
    public void update(Properties properties) {
        try {
            readWriteLock.writeLock().lockInterruptibly();

            try {
                // the property must be exist when update
                // 只整合 存在的属性
                mergeIfExist(properties, this.allConfigs);

                //将属性设置到 对应的配置对象中 只要找到对应的 以set开始的方法并设置属性
                for (Object configObject : configObjectList) {
                    // not allConfigs to update...
                    MixAll.properties2Object(properties, configObject);
                }

                //更新版本
                this.dataVersion.nextVersion();

            } finally {
                readWriteLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("update lock error, {}", properties);
            return;
        }

        //持久化
        persist();
    }

    public void persist() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {
                String allConfigs = getAllConfigsInternal();

                MixAll.string2File(allConfigs, getStorePath());
            } catch (IOException e) {
                log.error("persist string2File error, ", e);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist lock error");
        }
    }

    public String getAllConfigsFormatString() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                //输出所有的属性
                return getAllConfigsInternal();

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigsFormatString lock error");
        }

        return null;
    }

    public String getDataVersionJson() {
        return this.dataVersion.toJson();
    }

    public Properties getAllConfigs() {
        try {
            readWriteLock.readLock().lockInterruptibly();

            try {

                return this.allConfigs;

            } finally {
                readWriteLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getAllConfigs lock error");
        }

        return null;
    }

    private String getAllConfigsInternal() {
        StringBuilder stringBuilder = new StringBuilder();

        // reload from config object ?
        for (Object configObject : this.configObjectList) {
            Properties properties = MixAll.object2Properties(configObject);
            if (properties != null) {
                merge(properties, this.allConfigs);
            } else {
                log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
            }
        }

        {
            stringBuilder.append(MixAll.properties2String(this.allConfigs));
        }

        return stringBuilder.toString();
    }

    //整合属性
    private void merge(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            Object fromObj = from.get(key), toObj = to.get(key);
            //相同情况就替换
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            //不存在就加入新的属性
            to.put(key, fromObj);
        }
    }

    //只整合存在的属性
    private void mergeIfExist(Properties from, Properties to) {
        for (Object key : from.keySet()) {
            if (!to.containsKey(key)) {
                continue;
            }

            Object fromObj = from.get(key), toObj = to.get(key);
            if (toObj != null && !toObj.equals(fromObj)) {
                log.info("Replace, key: {}, value: {} -> {}", key, toObj, fromObj);
            }
            to.put(key, fromObj);
        }
    }

}
