package com.chicos.interfaces.customer;

import java.util.Collections;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.util.Iterator;
import java.util.List;

public class CustomerDAO {

    private Connection connection = DriverManager.getConnection("ojai:mapr:");
    private VBStore store;

    public VBStore getStore() {
        return store;
    }

    public CustomerDAO(String tabelPath, boolean print) {
        store = new VBStore (connection.getStore(tabelPath));
        store.setPrint(print);
    }

    public Iterator<Document> getIterator() {
        return store.find().iterator();
    }

    public Document get(String id) {
        return store.findById(id);
    }

    public void resetSequences( List<String> idArray) {

        for (String s : idArray) {
            String composedId = "test-" + s;

            Document existing = store.findById(composedId);
            if(existing != null)
                store.delete(composedId);
            store.insert(Json.newDocument().setId("test-" + s).set("sequence", Collections.emptyList()));
        }
    }
}
