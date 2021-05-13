package com.luck.utils;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.json.JSONObject;

public class RDDMultipleTextOutputFormat2 extends MultipleTextOutputFormat<String, JSONObject> {
    //自定义一个RDDMultipleTextOutputFormat2继承MultipleTextOutputFormat
    @Override
    public String generateFileNameForKeyValue(String key, JSONObject value,
                                              String name) {
        String object_type = value.getString("object_type");
        String object_id = value.getString("object_id");
        return object_type + "/" + object_id+".json";
    }

}
