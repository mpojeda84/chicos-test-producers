package com.chicos.interfaces.util;

import static java.lang.Integer.min;

public class NameKeyUtil {
  //helper function for addAssociaetMatchKeys -- used by associate interface as well
  public static String getNameKeyForAssociateMatchKey(String lastname, String firstname) {
    if (lastname == null) {
      lastname = "";
    }
    String lastnameFirstword = lastname.split("[-]")[0].toUpperCase();

    if (firstname == null) {
      firstname = "";
    }
    String firstTwoFirstname = firstname.substring(0, min(2, firstname.length())).toUpperCase();

    String name_key = lastnameFirstword + firstTwoFirstname;
    return name_key;
  }


}
