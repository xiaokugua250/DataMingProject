package com.morty.java.dmp.elasticsearch;

import com.sun.org.apache.bcel.internal.generic.InstructionConstants;
import groovy.json.JsonBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by Administrator on 2016/05/13.
 */
public class esSearchOpt {
    public Client client;
    BulkRequestBuilder bulkRequestBuilder;
    public SearchRequestBuilder builder;    //ES查询条件Builder
    public QueryBuilder queryBuilder;
    SearchResponse scrollResp;
    public  Settings settings;          //ES Settings

    public static org.apache.log4j.Logger LOG= org.apache.log4j.Logger.getLogger(esConnectionOpt.class);
    public void init(){
        client=esDescribeInfo.getClient(true);
        bulkRequestBuilder=client.prepareBulk();
        Properties properties=new Properties();
        properties.put("cluster.name",esDescribeInfo.ES_CLUSTER_NAME);
        properties.put("client.transport.sniff", "true");// 自动嗅探整个集群的状态,es会自动把集群中其它机器的ip地址加到客户端中
        settings= Settings.settingsBuilder().put(properties).build();

    }

    /**
     *
     * @param indexName
     * @param typeName
     * @param map
     * @return
     */
    public BulkResponse bulkSearch(String indexName,String typeName,Map<String,Object> map){
        try {
            bulkRequestBuilder.add(client.prepareIndex(
                    indexName,"1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .map(map)
                            .endObject()
                    ));
            bulkRequestBuilder.add(client.prepareIndex(indexName,typeName,"2")
                    .setSource(jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "another post")
                    .endObject()
            ));

            BulkResponse bulkResponse=bulkRequestBuilder.get();
            if(bulkResponse.hasFailures()){
                //TODO 处理返回错误
            }
            return  bulkResponse;
        } catch (IOException e) {
            e.printStackTrace();

        }
        return null;
    }

    /**
     *
     * @return
     */
    public SearchRequestBuilder getESQueryBuider(){
        builder=client.prepareSearch(esDescribeInfo.ES_INDEX_NMAE);     //搜索Index
        builder.setTypes(esDescribeInfo.ES_TYPE_NAME);      //搜索Type
        builder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);  //搜索类型
        //TODO 设置查询条件,builder.setXXX
        /* builder.setQuery(QueryBuilders.matchQuery("wareName","30liuliang"));// 设置查询条件
          builder.setPostFilter(FilterBuilders.rangeFilter("countValue").from(1000).to(5000));//设置过滤条件
          builder.addSort("countValue", SortOrder.ASC);//字段排序
          builder.addHighlightedField("wareName");//设置高亮字段
          builder.setHighlighterPreTags("<font red='colr'>");//设置高亮前缀
          builder.setHighlighterPostTags("</font>");//设置高亮后缀
          builder.setFrom(0);//pageNo 开始下标
          builder.setSize(2);//pageNum 共显示多少条
        * */
        return  builder;
    }

    /**
     * ES查询示例demo，其中查询条件可依据条件构建
     * @return response     es查询结果
     */
    public Object queryES(SearchRequestBuilder builder){
        try{
            SearchResponse response=builder.execute().actionGet();
            return response;
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("QUERY ERROR "+e.getMessage());
        }
        return null;
    }

    /**
     *
     * @param key
     * @param value
     * @param val
     * @return
     */
    public void scrollSearch(String key,String value,String val){
            queryBuilder=termQuery(key,value);
            scrollResp=client.prepareSearch(val)
                   // .addSort(FieldSortBuilder.EMPTY_PARAMS,SortOrder.ASC)
                    .setScroll(new TimeValue(60000))
                    .setQuery(queryBuilder)
                    .setSize(100).execute().actionGet();    ///100 hits per shard will be returned for each scroll
//Scroll until no hits are returned
        while (true){
            for(SearchHit hit:scrollResp.getHits().getHits()){
                //todo handle the hit
            }
            scrollResp=client.prepareSearchScroll(
                    scrollResp.getScrollId()).
                    setScroll(new TimeValue(600000)).
                    execute().
                    actionGet();
            //todo handle the hit
            if(scrollResp.getHits().getHits().length == 0){
                break;
            }
        }

    }

    public void multiSearch(){
        Node node=new Node(settings);
        SearchRequestBuilder srb1=node.client().prepareSearch().setQuery
                (QueryBuilders.queryStringQuery("elasticSearch")).setSize(1);
        SearchRequestBuilder srb2 = node.client()
                .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

        MultiSearchResponse sr=node.client().prepareMultiSearch().
                add(srb1)
                .add(srb2)
                .execute()
                .actionGet();
        
// You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits=0;
        for(MultiSearchResponse.Item item:sr.getResponses()){
            SearchResponse response=item.getResponse();
            nbHits+=response.getHits().getTotalHits();
        }
    }



}
