package com.lls.app.api;

import com.lls.app.core.Constants;
import com.lls.app.core.Task;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/************************************
 * CreatedDir
 * @author liliangshan
 * @date 2019/11/15
 ************************************/
public class CreatedDir implements Task {

    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(Constants.FS_DEFAULT_NAME, Constants.HD_FS_IP);

        FileSystem fs = FileSystem.get(conf);
        boolean isOk = fs.mkdirs(new Path("hdfs:/mydir"));
        if (isOk) {
            System.out.println("创建目录成功！");
        } else {
            System.out.println("创建目录失败！");
        }
        fs.close();
    }

}
