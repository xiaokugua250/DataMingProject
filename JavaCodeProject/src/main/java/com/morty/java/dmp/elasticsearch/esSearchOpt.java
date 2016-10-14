package com.morty.java.dmp.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by Administrator on 2016/05/13.
 */
public class EsSearchOpt {
    public static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(EsConnectionOpt.class);
    public Client                         client;
    public SearchRequestBuilder           builder;     // ES��ѯ����Builder
    public QueryBuilder                   queryBuilder;
    public Settings                       settings;    // ES Settings
    BulkRequestBuilder                    bulkRequestBuilder;
    SearchResponse                        scrollResp;

    /**
     *  agg ��ѯ
     * @param query
     * @param aggregationBuilder
     */
    public void aggSearch(String query, AggregationBuilder aggregationBuilder) {
        SearchResponse searchResponse;

        searchResponse = client.prepareSearch()
                               .setQuery(query)
                               .addAggregation(aggregationBuilder)
                               .execute()
                               .actionGet();
    }

    /**
     *
     * @param indexName
     * @param typeName
     * @param map
     * @return
     */
    public BulkResponse bulkSearch(String indexName, String typeName, Map<String, Object> map) {
        try {
            bulkRequestBuilder.add(client.prepareIndex(indexName, "1")
                                         .setSource(jsonBuilder().startObject().map(map).endObject()));
            bulkRequestBuilder.add(client.prepareIndex(indexName, typeName, "2")
                                         .setSource(jsonBuilder().startObject()
                                                                 .field("user", "kimchy")
                                                                 .field("postDate", new Date())
                                                                 .field("message", "another post")
                                                                 .endObject()));

            BulkResponse bulkResponse = bulkRequestBuilder.get();

            if (bulkResponse.hasFailures()) {

                // TODO �����ش���
            }

            return bulkResponse;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void init() {
        client             = EsDescribeInfo.getClient(true);
        bulkRequestBuilder = client.prepareBulk();

        Properties properties = new Properties();

        properties.put("cluster.name", EsDescribeInfo.ES_CLUSTER_NAME);
        properties.put("client.transport.sniff", "true");    // �Զ���̽������Ⱥ��״̬,es���Զ��Ѽ�Ⱥ������������ip��ַ�ӵ��ͻ�����
        settings = Settings.settingsBuilder().put(properties).build();
    }

    public void multiSearch() {
        Node                 node = new Node(settings);
        SearchRequestBuilder srb1 = node.client()
                                        .prepareSearch()
                                        .setQuery(QueryBuilders.queryStringQuery("elasticSearch"))
                                        .setSize(1);
        SearchRequestBuilder srb2 = node.client()
                                        .prepareSearch()
                                        .setQuery(QueryBuilders.matchQuery("name", "kimchy"))
                                        .setSize(1);
        MultiSearchResponse sr = node.client().prepareMultiSearch().add(srb1).add(srb2).execute().actionGet();

//      You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;

        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();

            nbHits += response.getHits().getTotalHits();
        }
    }

    /**
     * ES��ѯʾ��demo�����в�ѯ������������������
     * @return response     es��ѯ���
     */
    public Object queryES(SearchRequestBuilder builder) {
        try {
            SearchResponse response = builder.execute().actionGet();

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("QUERY ERROR " + e.getMessage());
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
    public void scrollSearch(String key, String value, String val) {
        queryBuilder = termQuery(key, value);
        scrollResp   = client.prepareSearch(val)

        // .addSort(FieldSortBuilder.EMPTY_PARAMS,SortOrder.ASC)
        .setScroll(new TimeValue(60000)).setQuery(queryBuilder).setSize(100).execute().actionGet();    // /100 hits per shard will be returned for each scroll

//      Scroll until no hits are returned
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {

                // todo handle the hit
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                               .setScroll(new TimeValue(600000))
                               .execute()
                               .actionGet();

            // todo handle the hit
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    /**
     *  AggregationsBuilders ����aggregationbuilde������agg��ѯ����
     * @param parmas
     * @return
     */
    public AggregationBuilder getAggregationBuider(String... parmas) {
        AggregationBuilder aggregationBuilder = null;

        aggregationBuilder = AggregationBuilders.terms(parmas[0]).field(parmas[1]).include(parmas[2]);

        return aggregationBuilder;
    }

    /**
     *
     * @return
     */
    public SearchRequestBuilder getESQueryBuider() {
        builder = client.prepareSearch(EsDescribeInfo.ES_INDEX_NMAE);    // ����Index
        builder.setTypes(EsDescribeInfo.ES_TYPE_NAME);            // ����Type
        builder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);    // ��������

        // TODO ���ò�ѯ����,builder.setXXX

        /*
         *  builder.setQuery(QueryBuilders.matchQuery("wareName","30liuliang"));// ���ò�ѯ����
         * builder.setPostFilter(FilterBuilders.rangeFilter("countValue").from(1000).to(5000));//���ù�������
         * builder.addSort("countValue", SortOrder.ASC);//�ֶ�����
         * builder.addHighlightedField("wareName");//���ø����ֶ�
         * builder.setHighlighterPreTags("<font red='colr'>");//���ø���ǰ׺
         * builder.setHighlighterPostTags("</font>");//���ø�����׺
         * builder.setFrom(0);//pageNo ��ʼ�±�
         * builder.setSize(2);//pageNum ����ʾ������
         */
        return builder;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
