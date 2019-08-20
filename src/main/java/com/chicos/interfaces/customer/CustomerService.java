package com.chicos.interfaces.customer;

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

public class CustomerService {

  public int setRandomInt(Document document, String path) {
    int value = new Random().nextInt(1000);
    this.set(document, path, value);
    return value;
  }

  public String setRandomString(Document document, String path) {
    String value = "string-" + new Random().nextInt(1000);
    this.set(document, path, value);
    return value;
  }


  public String getFirstConsolidationId(Document document) {
    List<Object> consolidations = document.getList("consolidations");
    if (consolidations != null && !consolidations.isEmpty()) {
      Map<String, Object> map = document.getMap("consolidations[0]");
      if (map.get("old_brand_id") != null && map.get("old_customer_no") != null) {
        return new StringBuffer()
            .append(document.getLong("consolidations[0].old_brand_id"))
            .append("+")
            .append(document.getLong("consolidations[0].old_customer_no"))
            .toString();
      }
    }
    return null;
  }


  private void set(Document document, String path, int value) {
    document.set(path, value);
  }

  private void set(Document document, String path, String value) {
    document.set(path, value);
  }


  public void replaceCustomerNoAndBrandIdFromId(Document document) {
    String id[] = document.getIdString().split("\\+");
    document.set("brand_id", Long.parseLong(id[0]));
    document.set("customer_no", Long.parseLong(id[1]));
    document.delete("_id");
  }

  public void setMarketingEmail(Document document, String email) {

    Map<String, Object> newEmail = new HashMap<String, Object>() {{
      put("type", "marketing");
      put("email_address", email);
    }};

    document.set("emails", Collections.singletonList(newEmail));
  }

  public void setVB(Document document, int vb) {
    document.set("vb", vb);
  }

  public void removeConsolidationsArray(Document document) {
    document.delete("consolidations");
  }

  public void removeAlternateKeysArray(Document document) {
    document.delete("alternate_keys");
  }

  public String extractAssociateMatchKeys(Document inputData) {

    String fieldname_addresses = "addresses";
    String fieldname_zip_code = "zip_code";

    List<Object> documentList = inputData.getList(fieldname_addresses);
    if (documentList == null) {
      return null;
    }

    String fieldname_address_type = "type";
    String fieldname_address_line1 = "street1";
    String fieldname_lastname = "street2";
    String fieldname_firstname = "first_name";

    String zip_key = "";
    String address_key = "";

    List<Document> addresses = new ArrayList(documentList);
    for (Document address : addresses) {

      //input input message as address_type as H change it to home
      String address_type = address.getString(fieldname_address_type);
      if (address_type == null || !address_type.equalsIgnoreCase("home")) {
        continue;
      }

      String zip_code = address.getString(fieldname_zip_code);
      String address_line1 = address.getString(fieldname_address_line1);
      Pair<String, String> p = ZipAddressUtil.getZipKeyAddressKey(zip_code, address_line1);
      zip_key = p.getFirst();
      address_key = p.getSecond();
      break;
    }

    String lastname = inputData.getString(fieldname_lastname);
    String firstname = inputData.getString(fieldname_firstname);
    String name_key = NameKeyUtil.getNameKeyForAssociateMatchKey(lastname, firstname);

    String associateMatchKey = zip_key + address_key + name_key;

    return associateMatchKey;
  }



}
