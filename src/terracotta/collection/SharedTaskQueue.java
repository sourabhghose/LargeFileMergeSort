package terracotta.collection;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.joda.time.DateTime;
 
/**
 * @author sourabh_gh
 *
 */
public class SharedTaskQueue
{
	// LinkedList might cause a race condition, if so use concurrentlinkedqueue or my concurrentlinkedlist implementation
	private Map<DateTime, LinkedList<String[]>> map = new ConcurrentSkipListMap<DateTime, LinkedList<String[]>>(new DateComparator());

	public void put(DateTime timeStamp, String[] data) throws InterruptedException
	{
		LinkedList<String[]> value = map.get(timeStamp);
		if(value == null){
			value = new LinkedList<String[]>();
			value.add(data);
			map.put(timeStamp, value);
		}
		else{
			value.add(data);
			map.put(timeStamp, value);
		}
	}

	public LinkedList<String[]> get(DateTime timeStamp) throws InterruptedException
	{
		return map.get(timeStamp);
	}

	public Map<DateTime, LinkedList<String[]>> getBackingMap(){
		return map;
	}
	
	public void setBackingMap(Map<DateTime, LinkedList<String[]>> map){
		this.map = map;
	}
	
	public String toString(){
		return map.toString();
	}
	
	public static class DateComparator implements Comparator<DateTime>{
		@Override
		public int compare(DateTime s1, DateTime s2){
			return s1.compareTo(s2);
		}
	}
}