package com.chicos.interfaces.customer;

import org.ojai.Document;

import java.util.*;

public class CustomerService {

    public int setRandomInt(Document document, String path) {
        int value = new Random().nextInt(1000);
        this.set(document,path, value);
        return value;
    }

    public String setRandomString (Document document, String path) {
        String value = "string-" + new Random().nextInt(1000);
        this.set(document,path, value);
        return value;
    }


    public String getFirstConsolidationId(Document document) {
        List<Object> consolidations = document.getList("consolidations");
        if(consolidations != null && !consolidations.isEmpty()) {
            Map map = document.getMap("consolidations[0]");
            if(map.get("old_brand_id") != null && map.get("old_customer_no") != null) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(document.getInt("consolidations[0].old_brand_id"));
                buffer.append("+");
                buffer.append((long)document.getLong("consolidations[0].old_customer_no"));
                return buffer.toString();
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

    public void setMarketingEmail(Document document, String email) {

        Map<String, Object> newEmail = new HashMap<>();
        newEmail.put("type", "Marketing");
        newEmail.put("email_address", email);
        document.set("emails", Collections.singletonList(newEmail));

    }



}
