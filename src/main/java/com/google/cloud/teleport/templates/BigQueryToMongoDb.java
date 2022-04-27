package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Write;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import com.google.cloud.teleport.templates.common.MongoDbUtils;
import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbOptions;
import com.google.cloud.teleport.templates.common.MongoDbUtils.BigQueryReadOptions;
import java.io.Serializable;

public class BigQueryToMongoDb implements Serializable{

    public interface Options extends PipelineOptions, GcpOptions, MongoDbOptions, BigQueryReadOptions {

        String getProjectId();

        void setProjectId(String value);
    }

    public static void main(String[] args) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> bigQueryDataset =
                pipeline.apply(
                        BigQueryIO
                                .readTableRows()
                                .withoutValidation()
                                .from(options.getInputTableSpec())
                );

        bigQueryDataset
                .apply("bigQueryDataset", MapElements.via
                        (new SimpleFunction<TableRow, Document>() {
                            @Override
                            public Document apply(TableRow row) {
                                Document doc = new Document();
                                row.forEach((key, value) ->
                                {
                                    doc.append(key, value);
                                });
                                return doc;
                            }
                        })
                )
                .apply(
                        MongoDbIO.write()
                                .withUri(MongoDbUtils.translateJDBCUrl(options.getMongoDbUri().get()))
                                .withDatabase(MongoDbUtils.translateJDBCUrl(options.getDatabase().get()))
                                .withCollection(MongoDbUtils.translateJDBCUrl(options.getCollection().get())));

        pipeline.run();
    }

}