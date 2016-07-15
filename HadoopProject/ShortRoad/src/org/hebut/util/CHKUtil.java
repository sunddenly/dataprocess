package org.hebut.util;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class CHKUtil{
	// a method used to check if can stop the jobs
	public static boolean canStop(String chkpath) throws IOException{
		String uri = chkpath; 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = fs.open(new Path(uri));
		DataInputStream datain = new DataInputStream(in);
		String temp = datain.readLine();
		String[] results = MyStringUtil.split(temp, "\t");
		int result = Integer.valueOf(results[1]);
		if(result > 0){
			return false;
		}
		else{
			return true;
		}
	}
}