#!/bin/bash
# -*- coding: gbk -*-


################################################
# File Name		: hadoop_job.sh
# Author		: liqibo(liqibo@baidu.com)
# Create Time	: 2017/08/01
# Brief			: hadoop job
################################################


set -x

# global config
# hadoop prefix
prefix_thang="hdfs://nmg01-taihang-hdfs.dmop.baidu.com:54310"
prefix_khan="hdfs://nmg01-khan-hdfs.dmop.baidu.com:54310"
prefix_mulan="hdfs://nmg01-mulan-hdfs.dmop.baidu.com:54310"
# hadoop bin
bin_thang="/home/liqibo/software/hadoop-client-taihang/hadoop/bin/hadoop"
bin_khan="/home/liqibo/software/hadoop-client-nmg/hadoop/bin/hadoop"
bin_mlan="/home/liqibo/software/mulan-hadoop-client/hadoop/bin/hadoop"
# shitu log
shitu_222_223="${prefix_khan}/app/dt/udw/release/app/fengchao/shitu/222_223"
shitu_400="${prefix_khan}/app/dt/udw/release/app/fengchao/shitu/400"
shitu_small="${prefix_khan}/app/ecom/fcr/dynamic_creative/dp/small_shitu/wise"

# using hadoop
hadoop_prefix="${prefix_khan}"
hadoop_bin="${bin_khan}"
hadoop_shitu="${shitu_222_223}"
# static date
[[ -z $1 ]] && static_date=`date -d "1 day ago" +"%Y%m%d"` || static_date=$1
# hadoop input dir
hadoop_input="${hadoop_shitu}/${static_date}/*/part-*"
# hadoop output dir
hadoop_output="${prefix_khan}/app/ecom/fcr/liqibo/test/test/${static_date}"

# local data
local_dir="/home/liqibo/liqibo/dev/hadoop"
path_mapper="${local_dir}/mapper.py"
path_reducer="${local_dir}/reducer.py"
path_file="${local_dir}/file"
# local merge data
path_merge_data="${local_dir}/data/${static_date}/merge_data"


function run_hadoop(){
	# clear output
	${hadoop_bin} fs -rmr ${hadoop_output}
	# hadoop streaming
		#-outputformat "org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat" \
		#-jobconf mapred.job.queue.name=fcr-adu \
	${hadoop_bin} streaming \
		-input ${hadoop_input} \
		-output ${hadoop_output} \
		-mapper "python27/bin/python2.7 mapper.py" \
		-reducer "python27/bin/python2.7 reducer.py file" \
		-file "${path_mapper}" \
		-file "${path_reducer}" \
		-file "${path_file}" \
		-cacheArchive "/share/python2.7.tar.gz#python27" \
		-jobconf mapred.job.name=liqibo-static \
		-jobconf mapred.job.priority=VERY_HIGH \
		-jobconf mapred.job.queue.name=fcr-adu \
		-jobconf stream.memory.limit=4096 \
		-jobconf mapred.job.map.capacity=3000 \
		-jobconf mapred.map.tasks=2000 \
		-jobconf mapred.job.reduce.capacity=2000 \
		-jobconf mapred.reduce.tasks=1000 \
		-jobconf mapred.job.reduce.memory.mb=5120 \
		-jobconf stream.num.map.output.key.fields=1

}


# main task
function main_task(){
	#echo ${hadoop_input}
	run_hadoop
	[[ -f ${path_merge_data} ]] && mv ${path_merge_data} ${path_merge_data}.bak
	${hadoop_bin} fs -getmerge ${hadoop_output} ${path_merge_data}
}


# run
#main_task






