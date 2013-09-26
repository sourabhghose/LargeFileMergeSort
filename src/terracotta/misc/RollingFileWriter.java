package terracotta.misc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.lang3.StringUtils;

import terracotta.FilePreprocessorRunner;
import terracotta.consumer.FileWriterConsumer;

/**
 * @author sourabh_gh
 *
 */
public class RollingFileWriter extends BufferedWriter{

	private static int numberOfLinesWritten = 0;
	private FileWriterConsumer consumer;

	public RollingFileWriter(Writer out, FileWriterConsumer consumer) {
		super(out);
		this.consumer = consumer;
	}

	@Override
	public void newLine() throws IOException{
		super.newLine();
		numberOfLinesWritten++;
		if(numberOfLinesWritten >= FilePreprocessorRunner.noOfLinesToWrite){
			try {
				super.close();
				FilePreprocessorRunner.outputFileName = StringUtils.replace(FilePreprocessorRunner.outputFileName, String.valueOf(consumer.getOutputFileNameIndex()), String.valueOf(consumer.getOutputFileNameIndex()+1));
				System.out.println(numberOfLinesWritten+" written, Rolling over to next file.. "+FilePreprocessorRunner.outputFileName);
				//consumer.bw = new RollingFileWriter(new FileWriter(FilePreprocessorRunner.dataPath+FilePreprocessorRunner.outputFileName, true), consumer);
				consumer.setOutputFileNameIndex(consumer.getOutputFileNameIndex()+1);
				numberOfLinesWritten = 0;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public int getNumberOfLinesWritten() {
		return numberOfLinesWritten;
	}

}
