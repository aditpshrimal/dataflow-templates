package org.example.analytics.common;

import com.google.api.client.json.JsonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;
import java.util.Set;
import java.util.TimeZone;

public class utils {
    private static final Logger LOG = LoggerFactory.getLogger(utils.class);
    private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();
    private static JsonParser jsonParser = new JsonParser();
    public static String parseFieldString(JsonObject jsonObject, String field) {
        String value = "";

        try {
            value = jsonObject.get(field).getAsString();
        } catch (Exception e) {
            return null;
        }
        return value;
    }

    public static long parseFieldLong(JsonObject jsonObject, String field) {
        long value = 0;

        try {
            value = Long.parseLong(jsonObject.get(field).getAsString());
        } catch (Exception e) {

        }
        return value;
    }

    public static float parseFieldFloat(JsonObject jsonObject, String field) {
        float value = 0.0f;

        try {
            value = Float.parseFloat(jsonObject.get(field).getAsString());
        } catch (Exception e) {

        }
        return value;
    }

    public static int parseFieldInteger(JsonObject jsonObject, String field) {
        int value = 0;

        try {
            value = Integer.parseInt(jsonObject.get(field).getAsString());
        } catch (Exception e) {

        }
        return value;
    }

    public static boolean parseFieldBoolean(JsonObject jsonObject, String field) {
        boolean value = true;

        try {
            value = Boolean.parseBoolean(jsonObject.get(field).getAsString());
        } catch (Exception e) {

        }
        return value;
    }

    public static JsonObject transformedJson(JsonObject jsonObject) {
        Calendar calendar = Calendar.getInstance();
        long rtc = calendar.getTimeInMillis();
        rtc = rtc / 1000;
        jsonObject = jsonFlattener(new JsonObject(), jsonObject, "");
        jsonObject.addProperty("rtc", rtc);

        return jsonObject;
    }
    public static JsonObject jsonFlattener(JsonObject result,JsonObject jsonObject,String property){
        Set<String> keys = jsonObject.keySet();
        for(String key: keys){
            if(!jsonObject.get(key).isJsonObject()){
                String field = "";
                if(property.equalsIgnoreCase("")) {
                    field = key;
                }else {
                    field = property+"_"+key;
                }
                result.add(field,jsonObject.get(key));
            }
            else {
                jsonFlattener(result,jsonObject.get(key).getAsJsonObject(),!property.equalsIgnoreCase("") ?property+"_"+key:key);
            }
        }
        return result;
    }

    public static Blob bucketExists(String bucketName, String eventName, String projectId) {

        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);
        int yy = calendar.get(Calendar.YEAR);
        String year = String.valueOf(yy);
        int mm = calendar.get(Calendar.MONTH) + 1;
        String month = String.format("%02d", mm);
        int dd = calendar.get(Calendar.DATE);
        String date = String.format("%02d", dd);
        int hh = calendar.get(Calendar.HOUR) - 1;
        String hour = String.format("%02d", (calendar.get(Calendar.AM_PM) == 0) ? hh : hh + 12);


        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        String directoryName = "events/" + eventName + "/" + year + "/" + month + "/" + date + "/";
        LOG.info("Hour" + hour);
        //System.out.println(directoryName);
        Bucket bucket = storage.get(bucketName);
        Page<Blob> blobs =
                bucket.list(
                        Storage.BlobListOption.prefix(directoryName),
                        Storage.BlobListOption.currentDirectory());


        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().equalsIgnoreCase(directoryName + hour + "/")) {
                LOG.info("Found blob:" + blob.getName());
                return blob;
            }

        }

        return null;
    }

    public static TableRow convertJsontToRow(String input){
        TableRow tableRow;
        try (InputStream inputStream =
                     new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))) {
            tableRow = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + input, e);
        }
        return tableRow;
    }
    public static TableRow failSafeElement(BigQueryInsertError bigQueryInsertError){
        TableRow tableRow = new TableRow();
        try {
            Calendar calendar = Calendar.getInstance();
            long ts = calendar.getTimeInMillis();
            ts = ts / 1000;

            JsonObject jsonObject = new JsonObject();
            String rowPayload = bigQueryInsertError.getRow().toString();
            jsonObject.addProperty("rowPayload",rowPayload);
            String errorString = bigQueryInsertError.getError().toString();
            JsonObject errorMessage = (JsonObject) jsonParser.parse(errorString);
            jsonObject.add("errors",errorMessage.get("errors"));
            jsonObject.add("index",errorMessage.get("index"));
            jsonObject.addProperty("timestamp",ts);

            tableRow = convertJsontToRow(jsonObject.toString());

        } catch (RuntimeException e) {
            System.out.println("Couldn't make failSafeElement for "+bigQueryInsertError.getRow().toString());
        }
        return tableRow;
    }

}
;