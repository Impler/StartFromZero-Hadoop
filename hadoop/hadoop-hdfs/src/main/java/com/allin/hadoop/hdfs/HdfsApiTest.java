package com.allin.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;

public class HdfsApiTest {

	/**
	 * FileSystem 实际是一个访问HDFS的客户端
	 * 
	 * @throws IOException
	 */
	@Test
	public void testFileSystem_local() throws IOException {

		Configuration conf = new Configuration();

		/**
		 * 根据Configuration 中配置的fs.defaultFS参数，确定创建FileSystem的类型
		 * 默认读取hadoop-common包下的core-default.xml配置的fs.defaultFS值 默认创建指向本地文件系统file:///
		 * 的LocalFileSystem
		 */
		FileSystem fs = FileSystem.get(conf);

		Assert.assertEquals(true, fs instanceof LocalFileSystem);
	}

	@Test
	public void testFileSystem_remote() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		FileSystem fs = FileSystem.get(conf);
		Assert.assertEquals(true, fs instanceof DistributedFileSystem);
	}

	@Test
	public void testCopyFromLocalFile() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		FileSystem fs = FileSystem.get(conf);
		Path targetPath = new Path("/hdfsapi/docker_practice.pdf");
		boolean exist = fs.exists(targetPath);
		System.out.println("path: " + targetPath + " exists? " + exist);
		if (exist) {
			boolean result = fs.delete(targetPath, true);
			System.out.println("delete path: " + targetPath + " success? " + result);
		}

		fs.copyFromLocalFile(new Path(this.getClass().getClassLoader().getResource("docker_practice.pdf").getPath()),
				targetPath);
		System.out.println("upload success");
	}

	@Test
	public void testcopyToLocalFile() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path("/hdfsapi/docker_practice.pdf");
		Path targetPath = new Path(
				this.getClass().getClassLoader().getResource("").toString() + System.currentTimeMillis() + ".pdf");
		fs.copyToLocalFile(srcPath, targetPath);
		fs.close();
		System.out.println("download " + srcPath + " to " + targetPath + " success");
	}
}
