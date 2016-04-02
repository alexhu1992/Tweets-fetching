package mxhHW2.mxhHW2;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Q4 {

	  public static class TopNMapper
	       extends Mapper<Object, Text, Text, IntWritable>{

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	String hash = itr.nextToken();
		    if(hash.indexOf("#")==0){
		        word.set(hash);
		        context.write(word, one);
		    }
	      }
	    }
	  }

	  public static class TopNReducer
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();
	    private Map<Text, IntWritable> countMap = new HashMap<>();
	    
	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	      countMap.put(new Text(key), new IntWritable(sum));
	    }
	    
	    @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
	  }
	  
  /*
 * sorts the map by values. Taken from:
 * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
 */
  private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
      List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

      Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
      	@Override
          public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
              return o2.getValue().compareTo(o1.getValue());
          }
      });

      //LinkedHashMap will keep the keys in the order they are inserted
      //which is currently sorted on natural ordering
      Map<K, V> sortedMap = new LinkedHashMap<K, V>();

      for (Map.Entry<K, V> entry : entries) {
          sortedMap.put(entry.getKey(), entry.getValue());
      }

      return sortedMap;
  }

}
 