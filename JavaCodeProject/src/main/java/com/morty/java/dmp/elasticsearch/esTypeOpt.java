package com.morty.java.dmp.elasticsearch;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.client.Client;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by Administrator on 2016/05/13.
 */
public class esTypeOpt {
    public PutIndexedScriptResponse putIndexedScriptResponse;
    public GetIndexedScriptResponse getIndexedScriptResponse;
    public DeleteIndexedScriptResponse deleteIndexedScriptResponse;
    public Client client;
    public CountResponse response;

    public void getDocCount(String indexName,String type){
        try {
            response=client.prepareCount(indexName)
                    .setQuery(termQuery("_type",type))
                    .execute()
                    .get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
