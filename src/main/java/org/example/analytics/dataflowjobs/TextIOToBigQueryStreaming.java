package org.example.analytics.dataflowjobs;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.example.analytics.common.JsonToRowConverter;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.Calendar;

public class TextIOToBigQueryStreaming {

    public interface CloudStorageToBigQueryStreamingOptions extends PipelineOptions {
        @Description("The files to read from.")
        @Validation.Required
        ValueProvider<String> getInputFileLocation();
        void setInputFileLocation(ValueProvider<String> value);

        @Description("Output table-name.")
        @Default.String("example")
        ValueProvider<String> getTableBaseName();
        void setTableBaseName(ValueProvider<String> value);
    }

    public static void main(String[] args) throws IOException {

        CloudStorageToBigQueryStreamingOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CloudStorageToBigQueryStreamingOptions.class);
        options.setTempLocation("gs://example-analytics/tmp/");

        String tableName = options.getTableBaseName().get();
        String inputFilePrefix = options.getInputFileLocation().get();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> event = pipeline.apply(TextIO.read().from(inputFilePrefix + "event/2023/03/18/*/*")
                .watchForNewFiles(Duration.standardMinutes(5), Watch.Growth.afterTimeSinceNewOutput(Duration.standardDays(7))));
        event.apply(MapElements.via(new SimpleFunction<String, TableRow>() {
                    @Override
                    public TableRow apply(String input) {
                        return JsonToRowConverter.jsonToRow(input, "event");
                    }
                })
        ).apply(BigQueryIO.writeTableRows().to(tableName + "event")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

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
