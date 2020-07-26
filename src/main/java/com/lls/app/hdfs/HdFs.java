package com.lls.app.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/************************************
 * HdFs
 * @author liliangshan
 * @date 2020/7/25
 ************************************/
public class HdFs {

    public static void main(String[] args) throws Exception {
        HdFs hdFs = new HdFs();
        hdFs.readFile();
    }

    public void readFile() throws Exception {
        // 注册HDFS流处理器
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        // 拿到HDFS文件系统的url地址
        // HDFS	NameNode	9000	fs.defaultFS	接收Client连接的RPC端口，用于获取文件系统metadata信息。
        // HDFS 访问协议配置在 Hadoop 中 core-site.xml  fs.defaultFS
        URL url = new URL("hdfs://liliangshan.local:9000/input/ali-test.kubeconfig");
        //拿到链接
        URLConnection con = url.openConnection();
        //读取文件，获取输入流
        InputStream inputStream = con.getInputStream();
        //创建缓冲区
        byte[] buf = new byte[inputStream.available()];
        //开始向缓冲区读取
        inputStream.read(buf);
        //读取完关闭输入流
        inputStream.close();
        //将缓冲区的内容转为字符串打印出来
        String str = new String(buf);
        System.out.println(str);
    }

}
