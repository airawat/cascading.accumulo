package com.talk3.cascading.accumulo;

import java.io.Serializable;
import java.util.LinkedList;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Text;

public class Utils {
	public static Text objToText(Object o)
    {
    	return new Text(objToBytes(o));
    }
    
    public static byte[] objToBytes(Object o)
    {
    	if (o instanceof String) {
			String str = (String) o;
			return str.getBytes();
		}
    	else if (o instanceof Long) {
			Long l = (Long) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Integer) {
    		Integer l = (Integer) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Boolean) {
    		Boolean l = (Boolean) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Float) {
    		Float l = (Float) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Double) {
    		Double l = (Double) o;
			return l.toString().getBytes();
		}
    	else if (o instanceof Text) {
    		Text l = (Text) o;
			return l.toString().getBytes();
		}
    	
    	return new String("").getBytes();
    }
}
