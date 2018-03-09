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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestPipesExternalSplits {

   /**
   *  test PipesNonJavaInputFormat External splits support.
   */

  @Test
  public void testExternalSplitsSupport() throws IOException, InterruptedException {
      Configuration conf = new Configuration();
      // FIXME this is hardwired. Use tmpfile or something like that.
      final String uri = new String("file:///tmp/a_binary_file.bin");
      conf.setBoolean(PipesNonJavaInputFormat.EXTERNAL_SPLITS_ENABLED, true);
      conf.setBoolean(PipesNonJavaInputFormat.EXTERNAL_SPLITS_URI, uri);

      PipesNonJavaInputFormat input_format = new PipesNonJavaInputFormat();
      List<InputSplit> written = write_input_splits(conf, uri);
      List<InputSplit> read = input_format.getSplits(conf);

      assertEquals(written.size(), read.size());
      for(int i = 0; i < read.size(); i++) {
          OpaqueSplit itwas = (OpaqueSplit) written[i];
          OpaqueSplit itis = (OpaqueSplit) read[i];
          assertEquals(itwas.code, itis.code);
          assertEquals(itwas.payload, itis.payload);          
      }
  }

  private List<InputSplit> write_input_splits(Configuration conf,
                                              String uri)
                                              throws IOException,
                                                     InterruptedException {  
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(uri);
      FSDataOutputStream out = fs.open(path, "w");
      IntWritable n_records = new IntWritable(10);
      n_records.write(out);
      List<InputSplit> splits = new ArrayList<InputSplit>();
      
      for(int i = 0; i < n_records.get(); i++) {
          String code = "code-" + i;
          String code = "payload-" + i;
          OpaqueSplit osplit = new OpaqueSplit(code.getBytes(), payload.getBytes());
          osplit.write(out);
          splits.add(osplit);
      }
      
      out.close();
      fs.close();
      return splits;
    }
    
  }
}
