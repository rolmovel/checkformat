package org.keedio;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class CheckFormat {

	
	public static void main(String ...args) throws InvocationTargetException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, ParseException, IOException{
		
		if (args.length!=2) {
			System.out.println("Parametros: fichero_regex fichero_texto");
			System.exit(0);
		}
		
		LineNumberReader regexR = new LineNumberReader(new FileReader(args[0]));
		LineNumberReader messageR = new LineNumberReader(new FileReader(args[1]));
		
		String check = getFilteredMessages(regexR.readLine(), messageR.readLine());
		System.out.println("Resultado:" );
		System.out.println(check);
		
	}
	
	private static String getFilteredMessages(String key, String message) throws ParseException, InvocationTargetException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException {
		
		Map<String, Map<String, String>> map = new HashMap<>();
		
		Pattern p = Pattern.compile(key);
		Map groups = getNamedGroups(p);
		
		Matcher m = p.matcher(message);
		
		if (groups.isEmpty()) {
			int j = 0;

			while (m.find()) {
				int count = m.groupCount();
				
				for (int i=1;i<=count;i++) {
					if (!map.containsKey("group" + j)) map.put("group" + j, new HashMap<String, String>());
					map.get("group" + j).put("item" + i, m.group(i));
				}
				
				j++;
			}

			return JSONObject.toJSONString(map);
		} else {
			int i=0;
			while (m.find()) {
				Iterator<String> it = groups.keySet().iterator();
				
				while (it.hasNext()) {
					String k = it.next();
					
					if (!map.containsKey("group" + i)) map.put("group" + i, new HashMap<String, String>());
					map.get("group" + i).put(k, m.group(k));
				}
				
				i++;
			}
			
			return JSONObject.toJSONString(map);

		}		
				
	}
	
	private static Map<String, Integer> getNamedGroups(Pattern regex)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
		namedGroupsMethod.setAccessible(true);

		Map<String, Integer> namedGroups = null;
		namedGroups = (Map<String, Integer>) namedGroupsMethod.invoke(regex);

		if (namedGroups == null) {
			throw new InternalError();
		}

		return Collections.unmodifiableMap(namedGroups);
	}


}
