package io.github.strmckr.apex.generator;

import io.github.strmckr.apex.engine.ApexEngine;

public class RecordCountForMode {
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
	                return ApexEngine.DEFAULT_RECORDS;
	        }
	    }
}
