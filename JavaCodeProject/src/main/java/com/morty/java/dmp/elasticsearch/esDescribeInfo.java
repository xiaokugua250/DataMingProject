package com.morty.java.dmp.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Created by Administrator on 2016/05/12.
 */
public class EsDescribeInfo {
    public static final String ES_CLUSTER_NAME = "Elasticsearch";    // ��Ⱥ����
    public static final String ES_INDEX_NMAE   = "ds-yili-v4";       // ���� ����
    public static final String ES_TYPE_NAME    = "ds-yili-v4";
    public static final String HOST_1          = "dev1";
    public static final String HOST_2          = "dev1";
    public static final String HOST_3          = "dev1";
    public static final String HOST_4          = "dev1";
    public static final String HOST_5          = "dev1";
    public static final String HOST_6          = "dev1";
    public static final int    PORT            = 9300;
    private static Client      client;
    private static Settings    settings;

    public static void init() {
        Properties properties = new Properties();

        properties.put("cluster.name", EsDescribeInfo.ES_CLUSTER_NAME);
        properties.put("client.transport.sniff", "true");    // �Զ���̽������Ⱥ��״̬,es���Զ��Ѽ�Ⱥ������������ip��ַ�ӵ��ͻ�����
        settings = Settings.settingsBuilder().put(properties).build();
    }

    // ����ģʽ��ȡclient
    public static synchronized Client getClient(boolean isDefault) {
        if (client == null) {
            try {
                if (isDefault == false) {
                    client = TransportClient.builder()
                                            .build()
                                            .addTransportAddress(
                                                new InetSocketTransportAddress(
                                                    InetAddress.getByName(EsDescribeInfo.HOST_1),
                                                    EsDescribeInfo.PORT))
                                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(EsDescribeInfo.HOST_2),
                                                                                                EsDescribeInfo.PORT));
                } else {
                    client = TransportClient.builder().settings(settings).build();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        return client;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
