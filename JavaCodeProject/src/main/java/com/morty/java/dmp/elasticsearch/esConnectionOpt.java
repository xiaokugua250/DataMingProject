package com.morty.java.dmp.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.Properties;

/**
 * Created by Administrator on 2016/05/12.
 */
public class EsConnectionOpt {
    public static org.apache.log4j.Logger LOG= org.apache.log4j.Logger.getLogger(EsConnectionOpt.class);
    public  Settings settings;          //ES Settings
    public SearchRequestBuilder builder;    //ES查询条件Builder
    private TransportClient client;       //ES Client

    public void init(){
        Properties properties=new Properties();
        properties.put("cluster.name", EsDescribeInfo.ES_CLUSTER_NAME);
        properties.put("client.transport.sniff", "true");// 自动嗅探整个集群的状态,es会自动把集群中其它机器的ip地址加到客户端中
        settings=Settings.settingsBuilder().put(properties).build();

    }
    /**
     *@param isDefault 是否为默认集群名称
     * @return client
     */
    public Client getClient(boolean isDefault){
        try{
            if(isDefault == false){
                client= TransportClient.builder().build().
                        addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EsDescribeInfo.HOST_1), EsDescribeInfo.PORT))
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EsDescribeInfo.HOST_2), EsDescribeInfo.PORT));
            }else {
                client=TransportClient.builder().settings(settings).build();
            }
            return  client;
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("CLIENT CREATE ERROE "+e.getMessage());
        }
            return  null;
    }

    public void closeClient(){
        try{
            client.close();
        }catch (Exception e){
            e.printStackTrace();
            LOG.debug("close error"+e.getMessage());
        }
    }







}
