package generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

public class dataparser {
	public  static DataMode parseMode(String arg) {
	        try {
	            return DataMode.valueOf(arg.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
	        } catch (IllegalArgumentException ex) {
	            throw new IllegalArgumentException("Unknown data mode '" + arg + "'. Valid modes: " +
	                    Arrays.toString(DataMode.values()), ex);
	        }
	    }

	public static ArrayList<DataMode> parseModes(String value) {
        String v = value.trim();
        ArrayList<DataMode> modes = new ArrayList<>();

        if (v.equalsIgnoreCase("all")) {
            modes.addAll(Arrays.asList(DataMode.values()));
            return modes;
        }

        String normalized = v.toUpperCase(Locale.ROOT).replace('-', '_');
        int range = normalized.indexOf("..");

        if (range >= 0) {
            DataMode start = dataparser.parseMode(normalized.substring(0, range));
            DataMode end = dataparser.parseMode(normalized.substring(range + 2));
            DataMode[] values = DataMode.values();
            int s = start.ordinal();
            int e = end.ordinal();

            if (s <= e) {
                for (int i = s; i <= e; i++) {
                    modes.add(values[i]);
                }
            } else {
                for (int i = s; i >= e; i--) {
                    modes.add(values[i]);
                }
            }

            return modes;
        }

        for (String part : v.split(",")) {
            if (!part.trim().isEmpty()) {
                modes.add(dataparser.parseMode(part));
            }
        }

        return modes;
    }	
	
	
}
