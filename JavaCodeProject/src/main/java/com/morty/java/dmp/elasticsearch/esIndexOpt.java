package com.morty.java.dmp.elasticsearch;

import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.client.Client;

/**
 * Created by Administrator on 2016/05/13.
 *
 * ES index script api
 */
public class EsIndexOpt {
    PutIndexedScriptResponse putIndexedScriptResponse;
    GetIndexedScriptResponse getIndexedScriptResponse;
    DeleteIndexedScriptResponse deleteIndexedScriptResponse;
    Client client;

    /**
     * ����SetXXX
     */
    public void deleteIndexedScriptOpt() {
        deleteIndexedScriptResponse = client.prepareDeleteIndexedScript()
                .setScriptLang("groovy")
                .setId("script1")
                .execute()
                .actionGet();
    }

    public void init() {
    }

    /**
     * ����SetXXX
     */
    public void getIndexedScriptOpt() {
        getIndexedScriptResponse = client.prepareGetIndexedScript()
                .setScriptLang("groovy")
                .setId("script1")
                .execute()
                .actionGet();
    }

    /**
     * ����setXXX
     */
    public void setPutIndexedScriptOpt() {
        putIndexedScriptResponse = client.preparePutIndexedScript()
                .setScriptLang("groovy")
                .setId("script1")
                .setSource("script", "_score * doc['my_numeric_field'].value")
                .execute()
                .actionGet();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
