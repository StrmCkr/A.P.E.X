package generator;

import main.Apex;

public class recordcountformode {
	public  static long recordCountForMode(DataMode mode) {
	        switch (mode) {
	            case EMPTY:
	                return 0;
	            case SINGLE_ELEMENT:
	                return 1;
	            case TWO_ELEMENTS_SORTED:
	            case TWO_ELEMENTS_REVERSED:
	                return 2;
	            default:
	                return Apex.DEFAULT_RECORDS;
	        }
	    }
}
