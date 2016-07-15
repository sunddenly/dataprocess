package org.hebut.util;
import java.util.*;
public class MyStringUtil{
	public static String[] split(String source,String spliter){
		int index=0;
		String tmp=source;
		Vector<String> vArray=new Vector<String>();
		while((index=tmp.indexOf(spliter))!=-1){
			vArray.add(tmp.substring(0,index));
			tmp=tmp.substring(index+spliter.length());
		}
		if(tmp.length()>0)vArray.add(tmp);
		String[] b=new String[vArray.size()];
		vArray.toArray(b);
		return b;
	}
	public static String byteToStr(byte[] b) {
		if (b.length == 0)
			throw new NullPointerException("data is null!");
		int pos = 0;
		for (int i = 0; i < b.length; i++) {
			if (b[i] == 0) {
				pos = i;
				break;
			}
		}
		byte[] bstr = new byte[pos];
		System.arraycopy(b, 0, bstr, 0, pos);
		String str = new String(bstr);
		return str;
	}
}