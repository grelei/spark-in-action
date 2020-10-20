package UDF;

import org.apache.calcite.rel.core.Collect;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class get_json_arrayUDF extends UDF {
    private static final Logger logger = LoggerFactory.getLogger(get_json_arrayUDF.class);
    private static final String SEPARTOR = "$.";
    private static final String LEFT_SEQUARE = "[";
    private static final String DOTS = ".";

    public List<String> evaluate(String str_json, String str_path) {
        str_path = str_path.replace(SEPARTOR, "");
        str_json = String.valueOf(str_json);

        /**如果传入参数为空，返回空List*/
        if (str_json == null || str_path == null) {
            return Collections.emptyList();
        }
        //建立一个空的List
        List<String> result = new ArrayList<>();
        int idx = str_path.indexOf(DOTS);
        String first = idx == -1 ? str_path : str_path.substring(0, idx);
        String next = idx == -1 ? null : str_path.substring(idx + 1);

        if (str_json.startsWith(LEFT_SEQUARE)) {
            /**解析json数组*/
            try {
                JSONArray arr = JSON.parseArray(str_json);
                //循环遍历该数组，递归调用自己
                for (int i = 0; i < arr.size(); i++) {
                    if (arr.getString(i) != null) {
                        result.addAll(this.evaluate(String.valueOf(arr.getString(i)), str_path));
                    }
                }
            } catch (Exception e) {
                logger.error("Parse Json Array Has error, The Source is {},Parse Path is {},The Exception is {}", str_json, str_path, e.getMessage());
            }
        } else {
            /**解析单个json实体*/
            try {
                JSONObject obj = JSON.parseObject(str_json);
                //获取最终的value值
                String resStr = obj.getString(first);
                /**当next == null时，说明已经到了path到了最后一级，获取该值，加入到List数组中，程序结束*/
                if (next == null) {
                    result.add(resStr);
                } else {
                    /**next <> null,说明不是最后一级，再次进入循环*/
                    if (resStr != null && next != null) {
                        result.addAll(this.evaluate(resStr, next));
                    }
                }
            } catch (Exception e) {
                logger.error("Parse Json Has error, The Source is {},Parse Path is {},The Exception is {}", str_json, str_path, e.getMessage());
            }
        }
        return result;
    }


    public static void main(String[] args) {
        String str = "{'code':'123'}";
        try {
            List<String> code = new get_json_arrayUDF().evaluate(str, "code");
            String result = StringUtils.join(code, "");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println(evaluate(JSON_OBJ_STR, "$.symptoms.name.name"));
    }

}