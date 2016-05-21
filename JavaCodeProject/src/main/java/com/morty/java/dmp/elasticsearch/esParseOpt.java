package com.morty.java.dmp.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 *
 * ES查询结果解析
 * Created by morty on 2016/05/20.
 */
public class esParseOpt {
    Logger LOG =Logger.getLogger(esParseOpt.class);

    //todo init
    public void init(){

    }

    /**
     *
     * @param response
     * @param params
     * @return
     */
    public Terms getAggTerms(SearchResponse response,String ... params){


        Terms terms=response.getAggregations().get("keys");

        //TODO 获取返回的response terms
     //   Terms terms=response.getAggregations().getAsMap();
     //   Terms terms=response.getXX();//


        return terms;
    }


    /**
     * 解析返回的terms
     * @param terms
     * @param parms
     */
    public void parseTerms(Terms terms,String ... parms){
         /*
         Terms  terms = response.getAggregations().get("keys");
        <Terms.Bucket> buckets = terms.getBuckets();

        for (Terms.Bucket bucket : buckets) {
          //  assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsText().string())));
            //// TODO: 2016/5/21  解析结果

        }*/
    }


}
