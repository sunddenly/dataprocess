package hadoopIo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.util.StringUtils;

public class IoTest {

	public static void strings(){
		String s="\u0041\u00df\u6771\uD801\uDc00";
		System.out.println(s.length());
		System.out.println(s.indexOf("\u0041"));
		System.out.println(s.indexOf("\u00DF"));
		System.out.println(s.indexOf("\u6771"));
		System.out.println(s.indexOf("\uD801\uDc00"));
		System.out.println("---------------------------");
	}
	public static void texts(){
		Text t = new Text("\u0041\u00df\u6771\uD801\uDc00");
		String s="\u0041\u00df\u6771\uD801\uDc00";
		System.out.println(t.getLength());
		System.out.println(t.find("\u0041"));
		System.out.println(t.find("\u00DF"));
		System.out.println(t.find("\u6771"));
		System.out.println(t.find("\uD801\uDc00"));
		
	}
	public static void main(String[] args) {
		strings();
		texts();
	}
}
