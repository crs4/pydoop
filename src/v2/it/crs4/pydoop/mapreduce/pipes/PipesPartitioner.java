/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.crs4.pydoop.mapreduce.pipes;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * This partitioner is one that can either be set manually per a record or it
 * can fall back onto a Java partitioner that was set by the user.
 */
class PipesPartitioner<K extends WritableComparable, V extends Writable>
    extends Partitioner<K, V> 
    implements Configurable {
  
    private static ThreadLocal<Integer> cache = new ThreadLocal<Integer>();
    private Partitioner<K, V> part = null;

    private Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
        part = ReflectionUtils.newInstance(
                   Submitter.getJavaPartitioner(conf), conf);
    }
  
    public Configuration getConf() {
        return conf;
    }

    /**
     * Set the next key to have the given partition.
     * @param newValue the next partition value
     */
    static void setNextPartition(int newValue) {
        cache.set(newValue);
    }

    /**
     * If a partition result was set manually, return it. Otherwise, we call
     * the Java partitioner.
     * @param key the key to partition
     * @param value the value to partition
     * @param numPartitions the number of reduces
     */
    @Override
    public int getPartition(K key, V value, 
                            int numPartitions) {
        Integer result = cache.get();
        if (result == null) {
            return part.getPartition(key, value, numPartitions);
        } else {
            return result;
        }
    }

}
