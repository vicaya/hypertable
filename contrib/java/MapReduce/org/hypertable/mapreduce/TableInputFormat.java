package org.hypertable.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.hypertable.mapreduce.TableSplit;
import org.apache.hadoop.conf.Configuration;

public class TableInputFormat implements InputFormat<Text, MapWritable>, JobConfigurable
{
  private Text m_tableName;
  private String m_rootPath;

  public TableInputFormat() {
    Configuration conf = new Configuration();
    m_rootPath = conf.get("hypertable.root.path",
        "/opt/hypertable/0.9.0.7/");
  }

  public void configure(JobConf job)
  {
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    m_tableName = new Text(tableNames[0].getName());
    job.set("hypertable.root.path", m_rootPath);
  }
  
  class DummyRecordReader implements RecordReader<Text, MapWritable> {
    public void close() { }
    public Text createKey() { return new Text(); }
    public MapWritable createValue() { return new MapWritable(); }
    public long getPos() { return 0; }
    public float getProgress() { return 0.0f; }
    public boolean next(Text k, MapWritable v) { return false; }
  }
  
  public RecordReader<Text, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) 
  throws IOException {
    return new DummyRecordReader();
  }
  
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String[] rangeVector = getRangeVector(m_tableName.toString(), m_rootPath);
    if (rangeVector == null || rangeVector.length == 0) {
      throw new IOException("No input split could be created.");
    }
    
    InputSplit[] splits = new InputSplit[rangeVector.length/3];
    
    for (int i = 0; i < (rangeVector.length); i+=3) {
      Text start = new Text(rangeVector[i]);
      Text end = new Text(rangeVector[i+1]);
      
      int u = rangeVector[i+2].indexOf("_");
      Text location = new Text(rangeVector[i+2].substring(0,u));
      
      splits[i] = new TableSplit(m_tableName, start, end, location);
    }

    return splits;
  }
 
  public void validateInput(JobConf job) {
    
  }

  static {
    System.loadLibrary("MapReduceJNI");
  }
  /**
    This JNI function returns a range vector for a
    given table in form of a string vector, where each
    odd field is a starting row and the next consecutive
    even field is an ending row.
  */
  private native String[] getRangeVector(String tableName, String configPath);
}

