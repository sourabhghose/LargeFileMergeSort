package terracotta.consumer;

import static terracotta.FilePreprocessorRunner.COMMA;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import terracotta.FilePreprocessorRunner;
import terracotta.collection.SharedTaskQueue;
import terracotta.producer.GPRSProducer;
import terracotta.producer.ThreeGProducer;
import terracotta.producer.TwoGProducer;

/**
 * @author sourabh_gh
 *
 */
public class FileWriterConsumer implements Runnable
{
    private SharedTaskQueue buffer;
    public static int outputFileNameIndex = 0;
    //public RollingFileWriter bw;
    private static int numberOfLinesWritten = 0;
 
    public FileWriterConsumer(SharedTaskQueue broker)
    {
        this.buffer = broker;
    }
 
    private DateTime compareDates(){
    	DateTime lower = TwoGProducer.getProcessedTill() == null ? ThreeGProducer.getProcessedTill() : (ThreeGProducer.getProcessedTill() == null ?
    			TwoGProducer.getProcessedTill() : (TwoGProducer.getProcessedTill().isBefore(ThreeGProducer.getProcessedTill()) ? 
    					TwoGProducer.getProcessedTill() : ThreeGProducer.getProcessedTill()));
    	DateTime lowest = lower == null ? GPRSProducer.getProcessedTill() : (GPRSProducer.getProcessedTill() == null ?
    			lower : (lower.isBefore(GPRSProducer.getProcessedTill()) ? 
    					lower : GPRSProducer.getProcessedTill()));
    	System.out.println("Lowest processed date.."+lowest);
    	return lowest;
    }
 
    @Override
    public void run()
    {
    	try
    	{
    		System.out.println("Consumer starting, buffer size is "+buffer.getBackingMap().size());
    		if(outputFileNameIndex == 0) {
    			FilePreprocessorRunner.outputFileName = StringUtils.replace(FilePreprocessorRunner.outputFileName, "$", String.valueOf(outputFileNameIndex));
    			System.out.println("Output file is.. "+FilePreprocessorRunner.outputFileName);
    		}
    		else {
    			FilePreprocessorRunner.outputFileName = StringUtils.replace(FilePreprocessorRunner.outputFileName, String.valueOf(outputFileNameIndex-1), String.valueOf(outputFileNameIndex));
    			System.out.println("Output file is.. "+FilePreprocessorRunner.outputFileName);
    		}
    		//bw = new RollingFileWriter(new FileWriter(FilePreprocessorRunner.dataPath+FilePreprocessorRunner.outputFileName, true), this);
    		BufferedWriter bw = new BufferedWriter(new FileWriter(FilePreprocessorRunner.dataPath+FilePreprocessorRunner.outputFileName, true));
    		DateTime processedTill = compareDates();
    		System.out.println("Flushing data till timestamp "+processedTill);
    		Iterator<Map.Entry<DateTime, LinkedList<String[]>>> iter = buffer.getBackingMap().entrySet().iterator();
    		DateTime currentKey;
    		while(iter.hasNext()){
    			Map.Entry<DateTime, LinkedList<String[]>> entry = iter.next();
    			currentKey = entry.getKey();
    			if(currentKey.compareTo(processedTill) == 0 || currentKey.compareTo(processedTill) == -1){
    				LinkedList<String[]> queue = entry.getValue();
    				int queueCount = 0;
    				for(Iterator<String[]> itr = queue.iterator();itr.hasNext();){
    					//writer.writeNext(itr.next());
    					String[] currLine = itr.next();
    					int length = currLine.length;
    					for(int i=0; i<length; i++){
    						bw.write(currLine[i]);
    						if(i==(length-1)){
    							bw.newLine();
    							numberOfLinesWritten++;
    							if(numberOfLinesWritten >= FilePreprocessorRunner.noOfLinesToWrite){
    								bw.close();
    								FilePreprocessorRunner.outputFileName = StringUtils.replace(FilePreprocessorRunner.outputFileName, String.valueOf(outputFileNameIndex), String.valueOf(outputFileNameIndex+1));
    								System.out.println(numberOfLinesWritten+" written, Rolling over to next file.. "+FilePreprocessorRunner.outputFileName);
    								bw = new BufferedWriter(new FileWriter(FilePreprocessorRunner.dataPath+FilePreprocessorRunner.outputFileName, true));
    								outputFileNameIndex++;
    								numberOfLinesWritten = 0;
    							}
    						}
    						else
    							bw.append(COMMA);
    					}
    					queue.remove(queueCount);
    					queueCount++;
    					//itr.remove();
    				}
    				//iter.remove();
    				buffer.getBackingMap().remove(currentKey);
    				queue = null;
    				currentKey = null;
    			}
    		}
    		System.out.println("Number of lines written "+numberOfLinesWritten);
    		if(buffer.getBackingMap().size() < 50 || buffer.getBackingMap().size() > 500 ){
    			buffer.getBackingMap().clear();
    			buffer.setBackingMap(new ConcurrentSkipListMap<DateTime, LinkedList<String[]>>(new SharedTaskQueue.DateComparator()));
    		}
    		//System.out.println("Final parameters output file "+FilePreprocessorRunner.outputFileName+" outputFileNameIndex "+outputFileNameIndex);
    		//System.out.println("Consumer sleeping for 5 secs, hopefully GC will clean up :)");
    		System.gc();
    		//Thread.sleep(5000);
    		System.out.println("Comsumer finished its job; terminating.");
    	}
//    	catch (InterruptedException e) {
//    		// TODO Auto-generated catch block
//    		e.printStackTrace();
//    	} 
    	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}