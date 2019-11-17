package com.lls.app.api;

import com.lls.app.core.Constants;
import com.lls.app.core.Task;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

/************************************
 * FileSystemCat
 * @author liliangshan
 * @date 2019/11/15
 ************************************/
public class FileSystemCat implements Task {

    // hadoop jar hp-world-1.0-SNAPSHOT.jar com.lls.app.Application
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(Constants.FS_DEFAULT_NAME, Constants.HD_FS_IP);

        FileSystem fs = FileSystem.get(conf);
        InputStream in = fs.open(new Path("hdfs:/file.txt"));
        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);

    }

}
