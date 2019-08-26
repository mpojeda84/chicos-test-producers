package com.chicos.interfaces.customer;

import com.chicos.interfaces.common.NonUniqueResultException;
import com.chicos.interfaces.common.VBStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.ojai.Document;
import org.ojai.json.Json;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;

import java.util.Iterator;
import java.util.List;
import org.ojai.store.Query;
import org.ojai.store.QueryResult;

public class CustomerDAO {

    private Connection connection = DriverManager.getConnection("ojai:mapr:");
    private VBStore store;

    public Connection getConnection() {
        return connection;
    }

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

    public void addToQuarantine(Map<Integer, List<String>> idsPerHash) {

        //1. find or create ids list
        Document quarantine = store.findById("quarantine");
        if(quarantine == null)
            store.insert(Json.newDocument().setId("quarantine").set("ids", Collections.emptyList()));
        quarantine = store.findById("quarantine");

        //2. create set to avoid duplicates
        List<String> ids = quarantine.getList("ids")
            .stream()
            .map(x -> (String)x)
            .collect(Collectors.toList());
        Set<String> existing = new HashSet<>(ids);

        //3.  add records with format quarantine-<id> as ID to the store
        //3.1 add ids to "existing" set, to use later
        idsPerHash.values()
            .stream()
            .flatMap(Collection::stream)
            .peek(existing::add)
            .map(x -> store.findById(x))
            .map(x ->  Json.newDocument().set("val", x.asJsonString()).setId("quarantine-" + x.getIdString()))
            .forEach(x -> {
                if(store.findById("quarantine-" + x.getIdString()) == null)
                    store.insertOrReplace(x);
            });

        // update the ids list with new "existing" set
        quarantine.set("ids", new ArrayList<>(existing));
        store.insertOrReplace(quarantine);

    }

    public void restoreFromQuarantine(){

        Document quarantine = store.findById("quarantine");
        if(quarantine == null)
            return;

        List<String> ids = quarantine.getList("ids").stream().map(x -> (String)x).collect(Collectors.toList());
        ids.stream()
            .map(x -> store.findById("quarantine-" + x).setId(x))
            .forEach(x -> {
                store.insertOrReplace( Json.newDocument(x.getString("val")));
                store.delete("quarantine-" + x.getIdString());
            });

        quarantine.set("ids", Collections.emptyList());
        store.insertOrReplace(quarantine);
    }

    public Document findByCustomerByAssociateKeys(String fieldName, String value) throws NonUniqueResultException {

        final Query query = connection.newQuery()
            .where("{\"$eq\": {\""+fieldName+"\": \""+value+"\"}}")
            .build();

        QueryResult queryResult = store.find(query);
        List<Document> result = new ArrayList<>();
        if(queryResult != null)
            queryResult.forEach(x -> {
                result.add(x);
            });
        if(result.size() > 1)
            throw new NonUniqueResultException();
        if(!result.isEmpty())
            return result.get(0);

        return null;
    }

}
