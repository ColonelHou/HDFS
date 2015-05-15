package com.cluster.hadoop.write;

import java.io.IOException;

public class Test {
	public static void main(String[] args) throws IOException {
		HdfsCommon hdfs = new HdfsCommon();
		hdfs.upFile(
				"C:/Users/Colonel.Hou/git/HDFS/HDFS/src/main/java/com/cluster/hadoop/HDFS/App.java",
				"hdfs://192.168.13.74:9000/data/log/app.java");
		// hdfs.downFile("hdfs://localhost:9000/user/whuqin/input/file01copy",
		// "/home/whuqin/fileCopy");
		// hdfs.appendFile("/home/whuqin/file01",
		// "hdfs://localhost:9000/user/whuqin/input/file01copy");
		// hdfs.delFile("hdfs://localhost:9000/user/whuqin/input/file01copy1");
	}
}