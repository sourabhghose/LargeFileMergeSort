package terracotta.misc;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * @author sghose
 *
 */
public class DataGenerator {

	private static String twogLine = "0x944324461a1b18d08d7ab25d9fa1595f,'TANGHLR1_13050112_9998',' ','','4','43',' ',' ','','','466974301229529','886983095821','','',' ',' ','','','2013-05-01 00:00:00.000',307,' ','0','0',' ','','','                        ',' ',' ','','',' ',' ','',' ',' ',' ','','1','2','1','986066686','','','','1','2','1','986066686',' ',' ','','','                ',' ',' ','','','      ','08-0-9-3    ','08-0-9-3    ','                                  ','TANGHLR1   ','17','0','  ',' ',' ',0,'IFET61','2-15','OEXR61','7-25','0','','','778158',0,0,'','',' ',,'','','',,,'','','','','','','',' ',0,'','','','','','','',' ',' ','','',' ','  ',' ','  ','8DCA28F284000007','1','1','1 ','886935***319','','','','','','',' ','  ','  ','2013-05-01 11:04:58.000',307,' ','',' ',181,'1','1','1 ','886935***319',' ','        ',' ',' ',' ','  ','  ','0','2','0','1','D9110935388202','','  ','','','',' ','',' ','  ','  ',,'','',' ',' ',' ',' ',' ',' ','  ','','','','','983095821','567683539688','986066686','329206559','','','','','','','986066686','329206559','',''";
	private static String threegLine = "0x3bbe61461a1b18d0b750b25d9fa1595f,'MCX01D_13050112_2938',1,1,1,1,1,1,1,1,0,2013-05-01 12:55:08.000',0,'2013-05-01 00:00:00.000','11','00','413B','55','0AD8','2013-05-01 12:47:59.000','03','FF','  ','FFFF','','466974104424565','','','','','','','','FF','939644958               ','05','06','970***660','05','06','65535',65535,,,'65535',,'FF',65535,,,'00','03','FFFF','351822059982110','466974700000830','','0000','0000','0000','0000','0000','0000','07','886983***686','05','05','8488',21111,466,97,'19546',886935***416,'05',21401,466,97,'FFFFFFFFFFFFFFFF',,'FF','FF','00000000','  ','3','8','2013-05-01 12:50:49.000','2013-05-01 12:48:08.000','F5B9','970***660','05','06','00','    ','FFFF','FF',0,'0970804097','05','04',0,0,0,0,0,0,'886935***374',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'  ','2013-05-01 12:47:59.000',,'',,,'00',0,0,0,0,0,0,0,0,0,'413B','55','0AD8','','FF','10','FF','00',0,0,'','  ','  ',,0,0,0,0,0,161,161.11,'01','0',0,'000000','08',,53,'1246',0,0,'FFFF','FFFFF','01','FF',436,62871450,'00','1','00','','  ','  ','FF','FF','FF','FF','FF','FF','FF',0,'2013-05-01 12:47:58.000',2,'00',,,,,,,,,'',,'      ','FFFF','FF','00','FF','FF','00','FF','2013-05-01 12:47:59.000','970***660','066208079','983***686','686791389688','970***660','066208079','','','',''";
	private static String gprsLine = "0x4c85ab201a1f18d093bfb25d9fa1595f,'CGW03_1305011035','18','0','3','1339655','466974301229529','2013-05-01 00:00:00.000','355026050423934','886983095821','103.2.219.41','61.31.42.39','1189235013','INTERNET','MNC097.MCC466.GPRS','1','100.66.203.15','41127','239','58844','1','8 ','46697 ','0','1','2',0,0,'02','FF','00','00','00','FE','00','00','00','00','00','4A','00','02','02','93','96','83','FE','74','81','FF','FF','00','00','00','2013-05-01 09:56:37.000','2013-05-01 10:03:03.000',386,'0 ','0',0,0,'00','00','00','00','00','00','00','00','00','00','00','00',,,0,'0','','0','0','','','00','0',0,0,0,0,,,,,'983095821','841295359688','00','00','00','3','1','1','0','0','0','',' ','',' ','','',' ','01306800','Apple','iPhone 4S','Handset','1','','','','','','0','','0','0'";
	
	
	private static int twoGSeed = 9;
	private static int threeGSeed = 9;
	private static int gprsSeed = 9;
	private static int totalNumberOf2gFiles;
	private static int totalNumberOf3gFiles;
	private static int totalNumberOfgprsFiles;
	
	private static String first2gFile = "CHAGHLR1_13082700_8853.csv";
	private static String first3gFile = "MCW02A_13082900_0183.csv";
	private static String firstgrpsFile = "CGW05_1308270005.csv";
	
	private static String twogstartTime = "2013-05-01 00:00:000.000";
	private static String threegstartTime = "2013-05-01 00:00:000.001";
	private static String gprsstartTime = "2013-05-01 00:00:000.002";
	

	public DataGenerator() throws InterruptedException, ExecutionException {
		ExecutorService service = Executors.newFixedThreadPool(3);

		Future<?> f1 = service.submit(new TwoGLoader(100000));
		Future<?> f2 = service.submit(new ThreeGLoader(100000));
		Future<?> f3 = service.submit(new GPRSLoader(100000));
		f1.get(); f2.get(); f3.get();

		service.shutdown();
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		if(args.length!=3){
			System.exit(1);
		}else{
			totalNumberOf2gFiles = Integer.parseInt(args[0]);
			totalNumberOf3gFiles = Integer.parseInt(args[1]);
			totalNumberOfgprsFiles = Integer.parseInt(args[2]);
		}
		new DataGenerator();
	}

	private static String generate2gTimeStamp() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long start = sdf.parse(twogstartTime).getTime();
		String date = sdf.format(start+twoGSeed);
		twoGSeed += 9;
		return "'"+date+"'";
	}
	
	private static String generate3gTimeStamp() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long start = sdf.parse(threegstartTime).getTime();
		String date = sdf.format(start+threeGSeed);
		threeGSeed += 9;
		return "'"+date+"'";
	}
	
	private static String generategprsTimeStamp() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long start = sdf.parse(gprsstartTime).getTime();
		String date = sdf.format(start+gprsSeed);
		gprsSeed += 9;
		return "'"+date+"'";
	}
	
	private static String increment2gFileName(){
		int start = first2gFile.indexOf("_");
		int end = first2gFile.lastIndexOf("_");
		int timeStamp = Integer.parseInt(first2gFile.substring(start+1, end));
		timeStamp+=15;
		first2gFile = "CHAGHLR1_"+timeStamp+"_8853.csv";
		System.out.println(first2gFile);
		return first2gFile;
	}
	
	private static String increment3gFileName(){
		int start = first3gFile.indexOf("_");
		int end = first3gFile.lastIndexOf("_");
		int timeStamp = Integer.parseInt(first3gFile.substring(start+1, end));
		timeStamp++;
		first3gFile = "MCW02A_"+timeStamp+"_0183.csv";
		System.out.println(first3gFile);
		return first3gFile;
	}
	
	private static String incrementgprsFileName(){
		int start = firstgrpsFile.indexOf("_");
		int end = firstgrpsFile.indexOf(".");
		int timeStamp = Integer.parseInt(firstgrpsFile.substring(start+1, end));
		timeStamp++;
		firstgrpsFile = "CGW05_"+timeStamp+".csv";
		System.out.println(firstgrpsFile);
		return firstgrpsFile;
	}

	private class TwoGLoader implements Runnable {
		private int noOfLines;
		private String[] twogLineArray = twogLine.split(",", -1);

		TwoGLoader(int noOfLines) {
			this.noOfLines = noOfLines;
		}

		public void run() {
			try {
				CSVWriter writer = new CSVWriter(new FileWriter("data/"+first2gFile, true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				while(totalNumberOf2gFiles!=0){
					for (int i = 0; i < noOfLines; i++) {
						twogLineArray[18] = generate2gTimeStamp();
						writer.writeNext(twogLineArray);
					}
					writer.close();
					totalNumberOf2gFiles--;
					writer = new CSVWriter(new FileWriter("data/"+increment2gFileName(), true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				}
			}catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private class ThreeGLoader implements Runnable {
		private int noOfLines;
		private String[] threegLineArray = threegLine.split(",", -1);

		ThreeGLoader(int noOfLines) {
			this.noOfLines = noOfLines;
		}

		public void run() {
			try {
				CSVWriter writer = new CSVWriter(new FileWriter("data/"+first3gFile, true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				while(totalNumberOf3gFiles!=0){
					for (int i = 0; i < noOfLines; i++) {
						threegLineArray[221] = generate3gTimeStamp();
						writer.writeNext(threegLineArray);
					}
					writer.close();
					totalNumberOf3gFiles--;
					writer = new CSVWriter(new FileWriter("data/"+increment3gFileName(), true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}

	private class GPRSLoader implements Runnable {
		private int noOfLines;
		private String[] gprsLineArray = gprsLine.split(",", -1);

		GPRSLoader(int noOfLines) {
			this.noOfLines = noOfLines;
		}

		public void run() {
			try {
				CSVWriter writer = new CSVWriter(new FileWriter("data/"+firstgrpsFile, true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				while(totalNumberOfgprsFiles!=0){
					for (int i = 0; i < noOfLines; i++) {
						gprsLineArray[7] = generategprsTimeStamp();
						writer.writeNext(gprsLineArray);
					}
					writer.close();
					totalNumberOfgprsFiles--;
					writer = new CSVWriter(new FileWriter("data/"+incrementgprsFileName(), true), ',', CSVWriter.NO_QUOTE_CHARACTER);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
}
