import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

//import org.apache.hadoop.hypertable.mapred.TableInputFormat;
//import org.apache.hadoop.hypertable.mapred.TableSplit;

public class TableMapTest {
  public static void main(String[] args) throws IOException {
    System.out.println("java.library.path="+System.getProperty("java.library.path")); 
    TableInputFormat t = new TableInputFormat("test");
    t.getSplits(null, 10);
  }
}
