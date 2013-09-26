package terracotta;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;

import terracotta.collection.SharedTaskQueue;
import terracotta.consumer.FileWriterConsumer;
import terracotta.producer.GPRSProducer;
import terracotta.producer.ThreeGProducer;
import terracotta.producer.TwoGProducer;

/**
 * @author sourabh_gh
 *
 */
public class FilePreprocessorRunner
{
	private Map<String, List<String>> twoGFileMap;
	private Map<String, List<String>> threeGFileMap;
	private Map<String, List<String>> gprsFileMap;
	private String first2gFile;
	private String first3gFile;
	private String firstgprsFile;
	private String twogFilePattern;
	private String threegFilePattern;
	private String gprsFilePattern;
	public static String dataPath;
	public static int twogtimestampcolumn;
	public static int threegtimestampcolumn;
	public static int gprstimestampcolumn;
	public static final char UNDERSCORE = '_';
	public static final String APOSTROPHE = "'";
	public static final String BLANK = "";
	public static final char DOT = '.';
	private static StringBuilder NOTUSED = new StringBuilder("X");
	public static final String COMMA = ",";
	public static final String QUOTE = "\"";
	public static int noOfLinesToWrite;
	public static String outputFileName;

	public FilePreprocessorRunner(){
		twoGFileMap = new TreeMap<String, List<String>>();
		threeGFileMap = new TreeMap<String, List<String>>();
		gprsFileMap = new TreeMap<String, List<String>>();
	}

	public static void main(String... args) throws IOException, InterruptedException, ExecutionException
	{
		final FilePreprocessorRunner runner = new FilePreprocessorRunner();
		runner.setUp();
		runner.buildFileMaps(dataPath);

		SharedTaskQueue broker = new SharedTaskQueue();
		//TODO - Change to Phaser so threads can deregister when file processing is complete
		CyclicBarrier barrier = new CyclicBarrier(3, new FileWriterConsumer(broker));
		ExecutorService threadPool = Executors.newFixedThreadPool(3);

		Future<?> twogstatus = threadPool.submit(new TwoGProducer(broker, barrier, runner.twoGFileMap));
		Future<?> threegstatus = threadPool.submit(new ThreeGProducer(broker, barrier, runner.threeGFileMap));
		Future<?> gprsstatus = threadPool.submit(new GPRSProducer(broker, barrier, runner.gprsFileMap));
		//Wait till threads have completed and flush remaining items from the buffer if any
		twogstatus.get();
		threegstatus.get();
		gprsstatus.get();

		//CSVWriter writer = new CSVWriter(new FileWriter("data/output.csv", true), ',', CSVWriter.NO_QUOTE_CHARACTER);
		BufferedWriter bw = new BufferedWriter(new FileWriter(dataPath+outputFileName, true));
		if(TwoGProducer.isDone() && ThreeGProducer.isDone() && GPRSProducer.isDone()){
			System.out.println("Producers have finished, flushing rest of the map..");
			Iterator<Map.Entry<DateTime, LinkedList<String[]>>> iter = broker.getBackingMap().entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry<DateTime, LinkedList<String[]>> entry = iter.next();
				LinkedList<String[]> queue = entry.getValue();
				for(Iterator<String[]> itr = queue.iterator();itr.hasNext();){
					//writer.writeNext(itr.next());
					String[] currLine = itr.next();
					int length = currLine.length;
					for(int i=0; i<length; i++){
						bw.write(currLine[i]);
						if(i==(length-1))
							bw.newLine();
						else
							bw.append(COMMA);
					}
				}
			}
		}
		if(bw!=null)
			bw.close();
		//writer.close();
		threadPool.shutdown();
	}

	private void setUp(){
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("conf/app.properties"));
			first2gFile = prop.getProperty("first2gfile");
			first3gFile = prop.getProperty("first3gfile");
			firstgprsFile = prop.getProperty("firstgprsfile");
			twogFilePattern = prop.getProperty("2gfilepattern");
			threegFilePattern = prop.getProperty("3gfilepattern");
			gprsFilePattern = prop.getProperty("gprsfilepattern");
			dataPath = prop.getProperty("datafilepath");
			twogtimestampcolumn = Integer.parseInt(prop.getProperty("twogtimestampcolumn"));
			threegtimestampcolumn = Integer.parseInt(prop.getProperty("threegtimestampcolumn"));
			gprstimestampcolumn = Integer.parseInt(prop.getProperty("gprstimestampcolumn"));
			outputFileName = prop.getProperty("outputFileName");
			noOfLinesToWrite = Integer.parseInt(prop.getProperty("noOfLinesToWrite"));

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private void buildFileMaps(String dataFilesPath){
		try {
			File folder = new File(dataFilesPath);

			File[] listOfFiles = folder.listFiles(); 
			List<String> _2gList = new ArrayList<String>();
			List<String> _3gList = new ArrayList<String>();
			List<String> _gprsList = new ArrayList<String>();

			int start = first2gFile.indexOf(UNDERSCORE);
			int end = first2gFile.lastIndexOf(UNDERSCORE);
			_2gList.add(first2gFile);
			twoGFileMap.put(first2gFile.substring(start+1, end), _2gList);

			start = first3gFile.indexOf(UNDERSCORE);
			end = first3gFile.lastIndexOf(UNDERSCORE);
			_3gList.add(first3gFile);
			threeGFileMap.put(first3gFile.substring(start+1, end), _3gList);

			start = firstgprsFile.indexOf(UNDERSCORE);
			end = firstgprsFile.indexOf(DOT);
			_gprsList.add(firstgprsFile);
			gprsFileMap.put(firstgprsFile.substring(start+1, end), _gprsList);
			
			for (File file : listOfFiles) 
			{
				if(file.getName().equals(first2gFile) || file.getName().equals(first3gFile) || file.getName().equals(firstgprsFile))
					continue;
				if (file.isFile()) 
				{
					//Build 2g
					String fileName = file.getName();
					if(fileName.contains(twogFilePattern)){
						start = fileName.indexOf(UNDERSCORE);
						end = fileName.lastIndexOf(UNDERSCORE);
						String key = fileName.substring(start+1, end);
						List<String> list = twoGFileMap.get(key);
						if(list==null){
							list = new ArrayList<String>();
							list.add(fileName);
							twoGFileMap.put(key, list);
						}
						else{
							list.add(fileName);
							twoGFileMap.put(key, list);
						}
					}
					//Build 3g
					else if(fileName.contains(threegFilePattern)){
						start = fileName.indexOf(UNDERSCORE);
						end = fileName.lastIndexOf(UNDERSCORE);
						String key = fileName.substring(start+1, end);
						List<String> list = threeGFileMap.get(key);
						if(list==null){
							list = new ArrayList<String>();
							list.add(fileName);
							threeGFileMap.put(key, list);
						}
						else{
							list.add(fileName);
							threeGFileMap.put(key, list);
						}
					}
					//Build gprs
					else if(fileName.contains(gprsFilePattern)){
						start = fileName.indexOf(UNDERSCORE);
						end = fileName.indexOf(DOT);
						String key = fileName.substring(start+1, end);
						List<String> list = gprsFileMap.get(key);
						if(list==null){
							list = new ArrayList<String>();
							list.add(fileName);
							gprsFileMap.put(key, list);
						}
						else{
							list.add(fileName);
							gprsFileMap.put(key, list);
						}
					}
					else{
						System.out.println("Unknown file, discarding.. "+fileName);
					}
				}
			}
			
			padFileMaps();
			
			System.out.println("GPRS Map "+gprsFileMap+ " \n3g map "+threeGFileMap+" \n2g map "+twoGFileMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void padFileMaps(){
		int gprsFileMapSize = gprsFileMap.size();
		int threeGFileMapSize = threeGFileMap.size();
		int twoGFileMapSize = twoGFileMap.size();
		int max = Math.max(Math.max(threeGFileMapSize, twoGFileMapSize), gprsFileMapSize);
		if(gprsFileMapSize<max){
			System.out.println("Padding Gprs map");
			int diff = max - gprsFileMapSize;
			for (int i=0; i< diff; i++){
				gprsFileMap.put(NOTUSED.append(i).toString(), null);
			}
		}
		if(threeGFileMapSize<max){
			System.out.println("Padding 3g map");
			int diff = max - threeGFileMapSize;
			for (int i=0; i< diff; i++){
				threeGFileMap.put(NOTUSED.append(i).toString(), null);
			}
		}
		if(twoGFileMapSize<max){
			System.out.println("Padding 2g map");
			int diff = max - threeGFileMapSize;
			for (int i=0; i< diff; i++){
				twoGFileMap.put(NOTUSED.append(i).toString(), null);
			}
		}
	}
}