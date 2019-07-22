package com.bia.sandbox.util;

import com.google.api.client.util.DateTime;

import javax.xml.datatype.XMLGregorianCalendar;

public class BQUtil {

    public static DateTime convertToBQDate(XMLGregorianCalendar xmlGregorianCalendar){
        try{
            return new DateTime(xmlGregorianCalendar.toGregorianCalendar().getTime());
        }catch (NullPointerException e){
            return null;
        }
    }

}
