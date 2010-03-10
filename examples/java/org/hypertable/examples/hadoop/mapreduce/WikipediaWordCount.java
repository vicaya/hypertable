/**
 * Copyright (c) 2010, Hypertable, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * - Neither Hypertable, Inc. nor the names of its contributors may be used to
 *   endorse or promote products derived from this software without specific
 *   prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.hypertable.examples;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.hypertable.hadoop.mapreduce.Helper;
import org.hypertable.hadoop.mapreduce.KeyWritable;
import org.hypertable.hadoop.mapreduce.ScanSpec;

import org.hypertable.Common.Time;

public class WikipediaWordCount {

  public static class TokenizerMapper 
       extends org.hypertable.hadoop.mapreduce.Mapper<KeyWritable, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
      
    public void map(KeyWritable key, BytesWritable value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(new String(value.getBytes()),
                                                " \t\n\r!\"#$%&'()*+-./`:;><=?@[\\]^_{|}~,");
      System.out.println("cf="+key.column_family+", ts="+new Date(key.timestamp/1000000));
      key.column_family = "word";
      key.setColumn_qualifierIsSet(true);
      while (itr.hasMoreTokens()) {
        key.column_qualifier = itr.nextToken();
        context.write(key, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<KeyWritable,IntWritable,KeyWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(KeyWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class IntSumTableReducer
       extends org.hypertable.hadoop.mapreduce.Reducer<KeyWritable, IntWritable>{
    
    public void reduce(KeyWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      byte [] vbytes = String.valueOf(sum).getBytes();
      BytesWritable output_value = new BytesWritable(vbytes);
      context.write(key, output_value);
    }
  }

  public static class Arguments {
    public ScanSpec scan_spec = new ScanSpec();
  }

  public static Arguments parseArgs(String[] args) throws ParseException {
    Arguments parsed_args = new Arguments();
    Date ts;
    for (int i=0; i<args.length; i++) {
      if (args[i].startsWith("--start-time=")) {
        ts = Time.parse_ts(args[i].substring(13));
        parsed_args.scan_spec.start_time = ts.getTime() * 1000000;
        parsed_args.scan_spec.setStart_timeIsSet(true);
      }
      else if (args[i].startsWith("--end-time=")) {
        ts = Time.parse_ts(args[i].substring(11));
        parsed_args.scan_spec.end_time = ts.getTime() * 1000000;
        parsed_args.scan_spec.setEnd_timeIsSet(true);
      }
      else if (args[i].startsWith("--columns=")) {
        StringTokenizer itr = new StringTokenizer(args[i].substring(10), ",");
        ArrayList<String> columns = new ArrayList<String>();
        while (itr.hasMoreTokens())
          columns.add(itr.nextToken());
        parsed_args.scan_spec.setColumns(columns);
        parsed_args.scan_spec.setColumnsIsSet(true);
      }
    }
    return parsed_args;
  }


  /**
   * Main entry point.
   * 
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Arguments parsed_args = parseArgs(args);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "wikipedia");
    job.setJarByClass(WikipediaWordCount.class);
    job.setCombinerClass(IntSumReducer.class);
    Helper.initMapperJob("wikipedia", parsed_args.scan_spec, TokenizerMapper.class,
                         KeyWritable.class, IntWritable.class, job);
    Helper.initReducerJob("wikipedia", IntSumTableReducer.class, job);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
