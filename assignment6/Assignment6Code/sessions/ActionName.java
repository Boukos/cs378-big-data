/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.sessions;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum ActionName { 
  DEALER_PAGE_VIEWED, DEALER_WEBSITE_VIEWED, MORE_PHOTOS_VIEWED, VIEWED_CARFAX_REPORT, VIEWED_CARFAX_REPORT_UNHOSTED, NONE  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"ActionName\",\"namespace\":\"com.refactorlabs.cs378.sessions\",\"symbols\":[\"DEALER_PAGE_VIEWED\",\"DEALER_WEBSITE_VIEWED\",\"MORE_PHOTOS_VIEWED\",\"VIEWED_CARFAX_REPORT\",\"VIEWED_CARFAX_REPORT_UNHOSTED\",\"NONE\"],\"default\":\"NONE\"}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}