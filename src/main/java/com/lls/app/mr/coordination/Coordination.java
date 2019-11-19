package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/************************************
 * Coordination
 * 基于物品的协同过滤算法实现
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class Coordination implements Runner {

    public static void main(String[] args) throws Exception {
        String nameNodeIp = "localhost";
        String hdfs = "hdfs://" + nameNodeIp + ":9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs);

        Map<String, String> pathMap = new HashMap<>();
        pathMap.put(Constants.DISTINCT_RUNNER_INPUT, "/input/coordination/distinct");
        pathMap.put(Constants.DISTINCT_RUNNER_OUTPUT, "/output/coordination/distinct");

        pathMap.put(Constants.SCORE_RUNNER_INPUT, pathMap.get("distinct_runner_output"));	//后面每一步的输入路径都是前一步的输出路径
        pathMap.put(Constants.SCORE_RUNNER_OUTPUT, "/output/coordination/score");

        pathMap.put(Constants.VIEWER_RUNNER_INPUT, pathMap.get("score_runner_output"));
        pathMap.put(Constants.VIEWER_RUNNER_OUTPUT, "/output/coordination/viewer");

        pathMap.put(Constants.SINGLE_SCORE_RUNNER_INPUT_I, pathMap.get("score_runner_output"));
        pathMap.put(Constants.SINGLE_SCORE_RUNNER_INPUT_II, pathMap.get("viewer_runner_output"));
        pathMap.put(Constants.SINGLE_SCORE_RUNNER_OUTPUT, "/output/coordination/single_score");

        pathMap.put(Constants.AVERAGE_RUNNER_INPUT, pathMap.get("single_score_runner_output"));
        pathMap.put(Constants.AVERAGE_RUNNER_OUTPUT, "/output/coordination/average");

        pathMap.put(Constants.TOP_RUNNER_INPUT, pathMap.get("average_runner_output"));
        pathMap.put(Constants.TOP_RUNNER_OUTPUT, "/output/coordination/top");

        Coordination coordination = new Coordination();
        coordination.run(conf, pathMap);
        System.out.println("finished!");
    }

    @Override
    public boolean run(Configuration config, Map<String, String> map) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        boolean isOK = true;
        // step1: 去重
        DistinctRunner distinctRunner = new DistinctRunner();
        if (!distinctRunner.run(config, map)) {
            isOK = false;
        }

        // step2: 计算用户评分矩阵
        ScoreRunner scoreRunner = new ScoreRunner();
        if (!scoreRunner.run(config, map)) {
            isOK = false;
        }

        // step3: 计算同现矩阵
        ViewerRunner viewerRunner = new ViewerRunner();
        if (!viewerRunner.run(config, map)) {
            isOK = false;
        }

        // step4: 计算单项评分=同现矩阵*评分矩阵
        SingleScoreRunner singleScoreRunner = new SingleScoreRunner();
        if (!singleScoreRunner.run(config, map)) {
            isOK = false;
        }

        // step5: 计算评分总和
        AverageRunner averageRunner = new AverageRunner();
        if (!averageRunner.run(config, map)) {
            isOK = false;
        }

        // step6: 评分排序取Top10
        TopRunner topRunner = new TopRunner();
        if (!topRunner.run(config, map)) {
            isOK = false;
        }
        return isOK;
    }
}
