package com.lls.app.mr.coordination;

import java.util.HashMap;
import java.util.Map;

/************************************
 * Constants
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public class Constants {

    static final String DISTINCT_RUNNER_INPUT = "distinct_runner_input";
    static final String DISTINCT_RUNNER_OUTPUT = "distinct_runner_output";

    static final String SCORE_RUNNER_INPUT = "score_runner_input";
    static final String SCORE_RUNNER_OUTPUT = "score_runner_output";

    static final String VIEWER_RUNNER_INPUT = "viewer_runner_input";
    static final String VIEWER_RUNNER_OUTPUT = "viewer_runner_output";

    static final String SINGLE_SCORE_RUNNER_INPUT_I = "single_score_runner_input_1";
    static final String SINGLE_SCORE_RUNNER_INPUT_II = "single_score_runner_input_2";
    static final String SINGLE_SCORE_RUNNER_OUTPUT = "single_score_runner_output";

    static final String AVERAGE_RUNNER_INPUT = "average_runner_input";
    static final String AVERAGE_RUNNER_OUTPUT = "average_runner_output";

    static final String TOP_RUNNER_INPUT = "top_runner_input";
    static final String TOP_RUNNER_OUTPUT = "top_runner_output";

    static Map<String, Integer> ACTIONS = new HashMap<>();
    static {
        ACTIONS.put("click", 1);        //浏览
        ACTIONS.put("collect", 2);      //收藏
        ACTIONS.put("cart", 3);         //放入购物车
        ACTIONS.put("alipay", 4);       //支付
    }

}
