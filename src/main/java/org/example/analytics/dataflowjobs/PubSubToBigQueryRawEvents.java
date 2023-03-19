package org.example.analytics.dataflowjobs;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.example.analytics.common.utils;

// Replace with your package name above
// ...

public class PubSubToBigQueryRawEvents {
    private static final JsonParser jsonParser=new JsonParser();
    public interface PubSubToBigQueryRawOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputSub();
        void setInputSub(ValueProvider<String> value);

        @Description("Output table-name.")
        @Default.String("your_project_name")
        ValueProvider<String> getTableBaseName();
        void setTableBaseName(ValueProvider<String> value);
    }


    public static void main(String[] args) {
        PubSubToBigQueryRawOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryRawEvents.PubSubToBigQueryRawOptions.class);

        //options.setStreaming(true);

        //options.setTempLocation("gs://bigquery-tmp-location/bigquery-tmp-files/");

        String inputSub = options.getInputSub().get();
        String tableName = options.getTableBaseName().get();

        Pipeline pipeline = Pipeline.create(options);


        PCollection<String> events = pipeline.apply("Read PubSub Messages From Sub 'events'", PubsubIO.readStrings().fromSubscription(inputSub));
        PCollection<String> transformedEvents =  events.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                String input = c.element();
                String json = "";
                try{
                    JsonObject jsonObject = (JsonObject) jsonParser.parse(input);
                    jsonObject = utils.transformedJson(jsonObject);
                    if (jsonObject.get("event").getAsString().equalsIgnoreCase("product_impression")) {
                        if (jsonObject.has("products")) {
                            if (jsonObject.get("products").isJsonArray()) {
                                JsonArray products = jsonObject.get("products").getAsJsonArray();
                                jsonObject.remove("products");
                                for (JsonElement product : products) {
                                    JsonObject singleProduct = product.getAsJsonObject();
                                    for (String productKey : singleProduct.keySet()) {
                                        JsonElement value = singleProduct.get(productKey);
                                        jsonObject.add(productKey, value);
                                    }
                                    json = jsonObject.toString();
                                    c.output(json);
                                }
                            }
                            else {
                                //Products field is not JsonArray
                            }
                        } else {
                            //Doesn't have products field
                        }
                    } else {
                        json = jsonObject.toString();
                        c.output(json);
                    }
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }));
        PCollection<TableRow> flattenedEvents = transformedEvents.apply(MapElements.via(new SimpleFunction<String, TableRow>() {
                    @Override
                    public TableRow apply(String input) {
                        TableRow tableRow;
                        tableRow = utils.convertJsontToRow(input);
                        //System.out.println(tableRow);
                        return tableRow;
                    }
                })
        );


        WriteResult writeResult =  flattenedEvents.apply(
                "WriteSuccessfulRecords",
                BigQueryIO.writeTableRows()
                        .to(tableName)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

       WriteResult errorWriteResult = writeResult
                .getFailedInsertsWithErr()
                .apply(MapElements.via(new SimpleFunction<BigQueryInsertError, TableRow>() {
                    @Override
                    public TableRow apply(BigQueryInsertError bigQueryInsertError) {
                        TableRow tableRow = utils.failSafeElement(bigQueryInsertError);
                        //System.out.println(tableRow);
                        return tableRow;
                    }
                }))
                .apply("WriteFailedRecords",BigQueryIO.writeTableRows()
                        .to(tableName+"_error_records")
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));


        // Execute the pipeline and wait until it finishes running.
        PipelineResult result = pipeline.run();
        try {
            result.getState();
            result.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            // do nothing
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

