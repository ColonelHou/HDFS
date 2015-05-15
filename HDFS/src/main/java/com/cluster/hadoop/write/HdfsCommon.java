package com.cluster.hadoop.write;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsCommon {
	private Configuration conf;
	private FileSystem fs;

	public HdfsCommon() throws IOException {
		conf = new Configuration();
		/**
		 * 不加如下这句会报如下这个错
		 * Wrong FS: hdfs://:9000/data, expected: file:///
		 */
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		fs = FileSystem.get(conf);
	}

	/**
	 * 上传文件，
	 * 
	 * @param localFile
	 *            本地路径
	 * @param hdfsPath
	 *            格式为hdfs://ip:port/destination
	 * @throws IOException
	 */
	public void upFile(String localFile, String hdfsPath) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(localFile));
		OutputStream out = fs.create(new Path(hdfsPath));
		
		 /*FileSystem fs = FileSystem.get(URI.create(dst), conf);
		  OutputStream out = fs.create(new Path(dst), new Progressable() {
		   public void progress() {
		    System.out.print(".");
		   }
		  });*/
		
		IOUtils.copyBytes(in, out, conf);
	}

	/**
	 * 附加文件
	 * 
	 * @param localFile
	 * @param hdfsPath
	 * @throws IOException
	 */
	public void appendFile(String localFile, String hdfsPath)
			throws IOException {
		InputStream in = new FileInputStream(localFile);
		OutputStream out = fs.append(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, conf);
	}
	
	/**
	 * 字符串写入HDFS中
	 * @param localFile
	 * @param hdfsPath
	 * @throws IOException
	 */
	public void writeStrToHDFS()
			throws IOException {
		Configuration conf = new Configuration();
		String serverPath = "hdfs://localhost:9000/data/test/abcd.txt";
		Path hdfsfile = new Path(serverPath);
		FileSystem hdfs = FileSystem.get(URI.create(serverPath), conf);
		// 根据上面的serverPath，获取到的是一个org.apache.hadoop.hdfs.DistributedFileSystem对象
		FSDataOutputStream out = hdfs.append(hdfsfile);
		for (int i = 0; i < 10; i++) {
			out.write( (i + " -------------\n ").getBytes());
		}
		out.close();
	}
	
	/**
	 * 从HDFS上删除文件
	 * @throws IOException
	 */
	public static void deleteFromHdfs() throws IOException {
		String dst = "hdfs://localhost:9000/data/test/abcd.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		fs.deleteOnExit(new Path(dst));
		fs.close();
	}
	
	/**
	 * 遍历HDFS上的文件和目录
	 * @throws IOException
	 */
	public static void getDirectoryFromHdfs() throws IOException {
		String dst = "hdfs://localhost:9000/data/test/abcd.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			System.out.println("name:" + fileList[i].getPath().getName()
					+ "/t/tsize:" + fileList[i].getLen());
		}
		fs.close();
	}
	
	/**
	 * 从HDFS上读取文件
	 * @throws IOException
	 */
	public static void readFromHDFS() throws IOException {
		String dst = "hdfs://localhost:9000/data/test/in/World.log";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));

		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			System.out.println(new String(ioBuffer));
			readLen = hdfsInStream.read(ioBuffer);
		}
		hdfsInStream.close();
		fs.close();
	}
	
	/**
	 * 下载文件
	 * 
	 * @param hdfsPath
	 * @param localPath
	 * @throws IOException
	 */
	public void downFile(String hdfsPath, String localPath) throws IOException {
		InputStream in = fs.open(new Path(hdfsPath));
		OutputStream out = new FileOutputStream(localPath);
		IOUtils.copyBytes(in, out, conf);
	}

	/**
	 * 删除文件或目录
	 * 
	 * @param hdfsPath
	 * @throws IOException
	 */
	public void delFile(String hdfsPath) throws IOException {
		fs.delete(new Path(hdfsPath), true);
	}
}
