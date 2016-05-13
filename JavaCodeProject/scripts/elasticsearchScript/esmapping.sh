#!/usr/bin/env bash

print_usage(){
    echo "sh esmapping.sh <ip and port> <index version>"
    echo "eg: sh esmapping.sh dev:9200 6"
    exit 1
}
#判断参数
if [$# = 0]|| [$1 = "help"]; then
  print_usage
fi

#获取参数
IP_PORT=$1
VERSION=$2

echo "Acting index mapping..."
echo "ip and port:$IP_PORT"
echo "index version: $VERSION"

host_ip_port=$IP_PORT
indexName=es-project-v$VERSION

#创建索引
 curl -XPUT http://$host_ip_port/$indexName -d '{
  "mappings":{
    "user":{
      "_all":{"enabled":false},
      "properties":{
        "title":{"type":"string", "doc_value":"true"},
        "name":{"type":"string", "doc_value":"true"},
        "age":{"type":"integer", "doc_value":"true"},
        "vip":{"type":"boolean", "doc_value":"true"},
        "address":{
          "properties":{
            "province":{
              "index":"not_analyzed",
              "doc_value":"true",
              "type":"string"
            },
            "city":{
              "index":"not_analyzed",
              "type":"string",
              "doc_value":"true"
            },
            "postcode":{
              "index":"not_analyzed",
              "type":"string",
              "doc_value":"true"
            }
          }
        }
      }
    },
    "blogpost":{
      "properties":{
        "title":{"type":"string", "doc_value":"true"},
        "body":{"type":"string", "doc_value":"true"},
        "user_id":{
          "type":"string",
          "index":"not_analyzed",
          "doc_value":"true"
        },
        "created":{
          "type":"date",
          "doc_value":"true",
          "format":"strict_date_optional_time||epoch_millis"
        }
      }
    },
    "location":{
      "properties":{
        "user_id":{
          "type":"string",
          "index":"not_analyzed",
          "doc_value":"true"
        },
        "geo":{
          "type":"geo_point",
          "doc_value":"true"
        }
      }
    }
  }
}'
 #finish
echo
echo
echo "mapping finsh!"
echo

