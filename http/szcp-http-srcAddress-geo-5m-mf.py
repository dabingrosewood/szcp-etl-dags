# -*- coding:utf-8 -*-  

import ConfigParser,os
from builtins import range
from airflow.operators.es_to_es import EsToEsTransfer
from airflow.models import DAG
from datetime import datetime, timedelta

file_dir=os.path.split(os.path.realpath(__file__))[0]
cf = ConfigParser.ConfigParser()
cf.read(file_dir+"/szcp.conf")
environment = cf.get("start_date", "environment")
if environment == "test":
    start_date = datetime(2017,2,17)
else:#real
    start_date = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

args = {
    'owner': 'fan.mo',
    'start_date':start_date 
}

dag = DAG(
    dag_id='szcp-http-srcAddress-geo-5m-mf', default_args=args,
    schedule_interval="1,11,21,31,41,51 * * * *",
    dagrun_timeout=timedelta(minutes=1))

query = """
{
    "size": 0,
    "aggs": {
        "volume_over_time": {
        "date_histogram": {
        "field": "startTime",
        "interval": "5m",
        "time_zone": "UTC",
        "min_doc_count": 1
      },
      "aggs": {
        "2": {
          "terms": {
            "field": "srcGeoCountry",
            "size": 50,
            "order": {
              "_count": "desc"
            }
          },
          "aggs": {
            "3": {
              "terms": {
                "field": "srcGeoRegion",
                "size": 50,
                "order": {
                  "_count": "desc"
                }
              },
              "aggs": {
                "4": {
                  "terms": {
                    "field": "srcGeoCity",
                    "size": 50,
                    "order": {
                      "_count": "desc"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  },
    "query": {
        "filtered": {
            "query": {
                "query_string": {
                    "query": "protocolType:HTTP AND securityEyeLogType:1 AND srcGeoCountry:*",
                    "analyze_wildcard": true
                }
            },
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "startTime": {
                                    "gte": "{execution_date}||-20m-1m",
                                    "lt": "{execution_date}||-1m"
                                }
                            }
                        }
                    ],
                    "must_not": []
                }
            }
        }
    }
}
"""
dest_map = {
    'map_path': 'aggregations.volume_over_time.buckets',
    'level': [
        {
            'path':'2.buckets',  #address of next level
            'key': 'key_as_string',
            'key_name': '@timestamp'

        },
        {
            'path':'3.buckets',
            'key':'key',
            'key_name': 'srcGeoCountry'
        },
        {
            'key':'key',
            'key_name': 'srcGeoRegion'
        }
        ],
    'attrs': {
        '_path': '4.buckets',
        '_id': 'key',
        'srcGeoCity':'key',
        'srcAddress_geo_num':'doc_count'
    }
}

run_this = EsToEsTransfer(
    task_id='szcp-http-srcAddress-geo-5m-mf', src_index='dpi-index-*', src_query=query,
	dest_map=dest_map,dest_index='szcp-speed-up-index',dest_type='http-srcAddress-geo-5m', dag=dag)

if __name__ == "__main__":
    dag.cli()
