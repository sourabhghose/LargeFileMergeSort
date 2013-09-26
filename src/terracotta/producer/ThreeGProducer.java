package terracotta.producer;

import static terracotta.FilePreprocessorRunner.APOSTROPHE;
import static terracotta.FilePreprocessorRunner.BLANK;
import static terracotta.FilePreprocessorRunner.COMMA;
import static terracotta.FilePreprocessorRunner.dataPath;
import static terracotta.FilePreprocessorRunner.threegtimestampcolumn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import terracotta.collection.SharedTaskQueue;

/**
 * @author sourabh_gh
 *
 */
public class ThreeGProducer implements Runnable
{
	private SharedTaskQueue broker;
	private CyclicBarrier barrier;
	private Map<String, List<String>> threeGFileMap;
	private static boolean isDone = false;
	private static DateTime processedTill;
	private static final String threegConstant = ",'3g'";

	public ThreeGProducer(SharedTaskQueue broker, CyclicBarrier barrier, Map<String, List<String>> threeGFileMap)
	{
		this.broker = broker;
		this.barrier = barrier;
		this.threeGFileMap = threeGFileMap;
	}

	public static DateTime getProcessedTill() {
		return processedTill;
	}

	public static boolean isDone(){
		return isDone;
	}

	@Override
	public void run()
	{
		try
		{
			//CSVReader reader;
			BufferedReader br = null;
			//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    		DateTimeFormatter sdf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
			String nextLine;
			String[] splittedLine;
			while (!threeGFileMap.isEmpty()){
				for(Iterator<Map.Entry<String, List<String>>> iter = threeGFileMap.entrySet().iterator(); iter.hasNext();){
					Map.Entry<String, List<String>> entry = iter.next();
					if(entry == null || entry.getValue() == null){
						processedTill = null;
						iter.remove();
						System.out.println("3g Producer finished the file job; waiting.");
						barrier.await();
//						if(count == 0){
//							barrier.reset();
//							System.out.println("3g reset");
//						}
						continue;
					}
					for(String fileName : entry.getValue()){
						System.out.println("3g processing file "+fileName);
						//        				reader = new CSVReader(new FileReader(FilePreprocessorRunner.dataPath+fileName));
						//        				while ((nextLine = reader.readNext()) != null) {
						br = new BufferedReader(new FileReader(dataPath+fileName));
						while ((nextLine = br.readLine()) != null) {
							nextLine.concat(threegConstant);
							splittedLine = nextLine.split(COMMA, -1);
    						//splittedLine = StringUtils.splitPreserveAllTokens(nextLine, COMMA);
    						//Date date = sdf.parse(StringUtils.replace(splittedLine[twogtimestampcolumn], APOSTROPHE, BLANK));
    						DateTime date = DateTime.parse( StringUtils.replace(splittedLine[threegtimestampcolumn], APOSTROPHE, BLANK), sdf ) ;
							processedTill = date;
							broker.put(date, splittedLine);
						}
						br.close();
						//reader.close();
					}
					iter.remove();
					System.out.println("3G Producer finished the file job; waiting. Processed till "+processedTill);
					barrier.await();
//					if(count == 0){
//						barrier.reset();
//						System.out.println("3g reset");
//					}
				}
			}
			isDone = true;
		}
		catch (InterruptedException ex)
		{
			ex.printStackTrace();
		} catch (BrokenBarrierException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}
}