package org.example.analytics.dataflowjobs;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.example.analytics.common.utils;
import org.joda.time.Duration;

public class PubSubToCloudStorageRaw {
    private static final JsonParser jsonParser=new JsonParser();
    public interface PubSubToCloudStorageOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(1)
        ValueProvider<Integer> getWindowSize();
        void setWindowSize(ValueProvider<Integer> value);

        @Description("Path of the output path including its filename prefix.")
        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);

    }

    public static void main(String[] args) {


        PubSubToCloudStorageOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToCloudStorageOptions.class);

        options.setStreaming(true);

        String inputTopic = options.getInputTopic().get();
        int windowSize = options.getWindowSize().get();
        String outputLocation = options.getOutput().get();
        int numShards = 1;

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> events = pipeline.apply("Read PubSub Messages From Subscription 'events'", PubsubIO.readStrings().fromTopic(inputTopic))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        String input = c.element();
                        String json = "";
                        try {
                            JsonObject jsonObject = (JsonObject) jsonParser.parse(input);
                            if (jsonObject.has("event")){
                                jsonObject = utils.transformedJson(jsonObject);

                                json = jsonObject.toString();
                                c.output(json);

                            }
                            else {
                                jsonObject.addProperty("event","default");
                                json = jsonObject.toString();
                                c.output(json);
                            }
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }));

        //event
        events.apply(Filter.by((SerializableFunction<String, Boolean>) input1 -> {
                    String event = "";
                    try {
                        JsonObject jsonObject = (JsonObject) jsonParser.parse(input1);
                        event = jsonObject.get("event").getAsString();
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }
                    return (event.equalsIgnoreCase("event"));
                })).apply(Window.into(FixedWindows.of(Duration.standardMinutes(windowSize))))
                .apply("Write event Files to GCS ", new WriteOneFilePerWindow(outputLocation+"/event", numShards));

        //default
        events.apply(Filter.by((SerializableFunction<String, Boolean>) input1 -> {
                    String event = "";
                    try {
                        JsonObject jsonObject = (JsonObject) jsonParser.parse(input1);
                        event = jsonObject.get("event").getAsString();
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }
                    return (event.equalsIgnoreCase("default"));
                })).apply(Window.into(FixedWindows.of(Duration.standardMinutes(windowSize))))
                .apply("Write identify Files to GCS ", new WriteOneFilePerWindow(outputLocation+"/default", numShards));


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