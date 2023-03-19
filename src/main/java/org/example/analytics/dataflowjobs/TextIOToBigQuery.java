package org.example.analytics.dataflowjobs;

import com.google.cloud.storage.Blob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.PCollection;
import org.example.analytics.common.JsonToRowConverter;
import org.example.analytics.common.utils;
import java.io.IOException;

public class TextIOToBigQuery {

    public interface CloudStorageToBigQueryOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getBucketName();
        void setBucketName(ValueProvider<String> value);

        @Default.String("false")
        String getRunValue();
        void setRunValue(String value);

        @Description("Output table-name.")
        @Default.String("project_table_base_name")
        ValueProvider<String> getTableBaseName();
        void setTableBaseName(ValueProvider<String> value);
    }

    public static void main(String[] args) throws IOException {
        CloudStorageToBigQueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CloudStorageToBigQueryOptions.class);

        String projectId = options.getProjectId().get();
        options.setTempLocation("gs://bigquery-tmp-location/bigquery-tmp-files/");
        String bucketName = options.getBucketName().get();
        String tableName = options.getTableBaseName().get();

        Pipeline pipeline = Pipeline.create();

        if (options.getRunValue().equalsIgnoreCase("true")) {
            // event
            Blob event_blob = utils.bucketExists(bucketName, "event", projectId);
            if (event_blob != null) {
                PCollection<String> event = pipeline.apply(TextIO.read().from("gs://" + bucketName + "/" + event_blob.getName() + "*"));
                event.apply(MapElements.via(new SimpleFunction<String, TableRow>() {
                            @Override
                            public TableRow apply(String input) {
                                return JsonToRowConverter.jsonToRow(input, "event");
                            }
                        })
                ).apply(BigQueryIO.writeTableRows()
                        .to(tableName + "event")
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
            }
        }

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
