package org.example.analytics.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Set;

public class JsonToRowConverter {

    public static String experiment_id,category,action,label,value;
    private static final JsonParser jsonParser=new JsonParser();

    public static TableRow jsonToRow(String inputString,String tableName){
        TableRow tableRow = new TableRow();
        JsonObject jsonObject = (JsonObject) jsonParser.parse(inputString);


        //Common_fields
        long rtc = utils.parseFieldLong(jsonObject, "rtc");
        String ab_id = utils.parseFieldString(jsonObject,"ab_id");
        String session_id = utils.parseFieldString(jsonObject, "session_id");
        String user_id = utils.parseFieldString(jsonObject, "user_id");
        String timestamp = utils.parseFieldString(jsonObject, "timestamp");
        String page_type = utils.parseFieldString(jsonObject, "page_type");
        String event = utils.parseFieldString(jsonObject, "event");
        String platform = utils.parseFieldString(jsonObject, "platform");

        tableRow.set("rtc", rtc);
        tableRow.set("ab_id",ab_id);
        tableRow.set("session_id", session_id);
        tableRow.set("user_id", user_id);
        tableRow.set("timestamp", timestamp);
        tableRow.set("page_type", page_type);
        tableRow.set("event", event);
        tableRow.set("platform", platform);


        switch (tableName){

            case "event":
                category = utils.parseFieldString(jsonObject,"category");
                label = utils.parseFieldString(jsonObject,"label");
                action = utils.parseFieldString(jsonObject,"action");
                value = utils.parseFieldString(jsonObject,"value");
                experiment_id = utils.parseFieldString(jsonObject,"experiment_id");

                tableRow.set("category", category);
                tableRow.set("label", label);
                tableRow.set("action", action);
                tableRow.set("value", value);
                tableRow.set("experiment_id", experiment_id);

                break;
        }


        return tableRow;
    }
}