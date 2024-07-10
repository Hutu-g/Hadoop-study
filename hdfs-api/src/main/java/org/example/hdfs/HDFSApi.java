package org.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: hutu-g
 * @Date: 2024/7/10 15:44
 */
public class HDFSApi {

    //hadoop的文件系统
    private FileSystem fs;
    //hadoop中hdfs的配置
    private Configuration configuration = new Configuration();

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //获取连接，hadoop的通信端口
        URI uri = new URI("hdfs://hadoop101:8020");
        String user = new String("hadoop01");
        fs = FileSystem.get(uri, configuration, user);
    }
    //创建目录
    @Test
    public void textMkdir() throws IOException {
        fs.mkdirs(new Path("/hdfsdemo"));
    }
    //上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //参数解释：（是否删除原数据，是否允许覆盖，原数据路径，上传路径）
        fs.copyFromLocalFile(false, true, new Path("E:\\bigdata_project\\bigdata_project\\data\\a.txt"), new Path("/hdfsdemo"));
    }
    //下载
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
        //参数的解读:(原文件是否删除,原文件路径HDFS,日标地址路径,是否关闭本地校验)
        fs.copyToLocalFile(false,new Path("/hdfsdemo/a.txt"),new Path("E:\\bigdata_project\\bigdata_project\\data\\out\\a.txt"),true);
    }
    //删除
    @Test
    public void testRm() throws IOException {
        //参数的解读:(删除文件路径，是否递归删除)
        fs.delete(new Path("/hdfsdemo/gq125.txt"),false);
    }

    //移动更名
    @Test
    public void testMv() throws IOException {
        //参数的解读:(源文件路径，目标文件路径)
        //移动,更名,改路径名字
        fs.rename(new Path("/hdfsdemo/gq125.txt"),new Path("/hdfsdemo/gq.txt"));
    }

    //查看详情信息
    @Test
    public void testList() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("===="+fileStatus.getPath()+"====");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
        }
    }

    //判断文件夹还是文件
    @Test
    public void testIsDir() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

    }



    //创建文件，写入文字
    @Test
    public void textMkfile() throws IOException {
        try {
            Path filenamePath = new Path("/hdfsdemo/gq125.txt");
            if (fs.exists(filenamePath)) {
                fs.delete(filenamePath, true);
            }
            FSDataOutputStream out = fs.create(filenamePath);
            out.writeUTF("212026125");
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //读取
    @Test
    public void textReadFile() throws IOException {
        InputStream in = null;
        try {
            Path file = new Path("/hdfsdemo/a.txt");
            in = fs.open(file);
            IOUtils.copyBytes(in, System.out, 80192, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }


    @After
    public void close() throws IOException {
        //关闭连接
        fs.close();
    }

}
