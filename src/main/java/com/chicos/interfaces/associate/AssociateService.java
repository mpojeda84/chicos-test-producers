package com.chicos.interfaces.associate;

import static java.lang.Integer.min;

import com.chicos.interfaces.common.NameKeyUtil;
import com.chicos.interfaces.common.Pair;
import com.chicos.interfaces.common.ZipAddressUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.ojai.Document;

public class AssociateService {

  public String getAssociateMatchKey(Document inputData) {

    String fieldname_zip_code = "zip_code";
    String fieldname_address_line1 = "street1";
    String fieldname_lastname = "last_name";
    String fieldname_firstname = "first_name";

    String zip_code = inputData.getString(fieldname_zip_code);
    String address_line1 = inputData.getString(fieldname_address_line1);

    Pair<String, String> p = ZipAddressUtil.getZipKeyAddressKey(zip_code, address_line1);
    String zip_key = p.getFirst();
    String address_key = p.getSecond();


    //Set CustomerAlphakey
    String lastname = inputData.getString(fieldname_lastname);

    String firstname = inputData.getString(fieldname_firstname);

    String name_key = NameKeyUtil.getNameKeyForAssociateMatchKey(lastname, firstname);
    String associateMatchKey = zip_key + address_key + name_key;

    return associateMatchKey;
  }


}
