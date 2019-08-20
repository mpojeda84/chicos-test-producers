package com.chicos.interfaces.common;

import static java.lang.Integer.min;

public class ZipAddressUtil {

  public static Pair<String, String> getZipKeyAddressKey(String zip_code, String address_line1) {
    if (zip_code == null) {
      zip_code = "";
    }
    zip_code.replaceAll("[^a-zA-Z0-9]", "");
    String zip_key = zip_code.substring(0, min(zip_code.length(), 5));
    String address_key = "";

    if (address_line1 == null) {
      address_line1 = "";
    }
    address_line1 = address_line1.toUpperCase();

    String address_line1_alphanum = address_line1.replaceAll("[^a-zA-Z0-9]", "");
    if (address_line1_alphanum.length() > 4 && address_line1_alphanum.substring(0, 5)
        .equals("POBOX")) {
      address_key = (address_line1_alphanum + "XXXXX").substring(0, 10);
    } else {
      address_line1 = address_line1.replaceAll("[^a-zA-Z0-9 -]", "");
      String[] tokens = address_line1.split("[- ]");
      if (tokens.length > 1) {
        String secondWord = tokens[1];
        int minLength2ndWord = min(secondWord.length(), 7);
        address_key = secondWord.substring(0, minLength2ndWord);
      }
      if (tokens.length > 2 && address_key.length() < 3) {
        String thirdWord = tokens[2];
        int minLength3rdWord = min(thirdWord.length(), 7);
        address_key = thirdWord.substring(0, minLength3rdWord);
      }
      if (tokens.length > 2 && (tokens[0] + tokens[1] + tokens[2]).length() < 10) {
        address_key = (tokens[0] + tokens[1] + tokens[2] + "XXXXXXXXXX").substring(0, 10);
      }
    }
    return new Pair(zip_key, address_key);
  }

}
