#!/usr/bin/env bash
print_func_usage(){
    echo " sh esRun.sh <ip and port> <index version>"
    echo " eg : sh esRun.sh create 127.0.0.1:9200 /workspace/data/indexMapping.json"
    echo "eg :sh esRun.sh delete 127.0.0.1:9200 6"
    exit 1
}
#»ñÎýADN=$1
IP_PORT=$2
INDEX_NAME=$3
MAPPINGFILEPATH=$4

echo "ACTING ES INDEXING MAPPING...."
echo "IP AND PORT:$IP_PORT"
echo "INDEX VERSION:$VERSION"
host_ip_port=$IP_PORT
indexName=es_project_$INDEX_NAME
#´´½¨ËÒ
#curl -XPUT http://$host_ip_port/$indexName -d $MAPPINGFILEPATH

# ɾ³ý#curl -XDELETE   http://$host_ip_port/$indexName -d $MAPPINGFILEPATH


FUNC_CREATE_INDEX(){
    echo "----------´´½¨ESËÒ-------------"
        curl -XPUT http://$host_ip_port/$indexName -d $MAPPINGFILEPATH
    echo "-----------Í³ɴ´½¨--------------"
}

FUNC_DELETE_INDEX(){
    echo "---------ɾ³ý--------"
    curl -XDELETE   http://$host_ip_port/$indexName
    echo "----------Í³É¾³ý-----"
# ²Îýif
}
# ²Îýif
if [ $# = 0 ] || [ $1 == "help" ] 
then
	print_func_usage
elif [ $1 == "create" ]
then 
	FUNC_CREATE_INDEX
elif [ $1 == "delete" ] 
then 
	FUNC_DELETE_INDEX
fi


