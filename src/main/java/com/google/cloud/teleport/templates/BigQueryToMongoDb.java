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
import com.google.cloud.teleport.templates.common.MongoDbUtils.BigQueryOptions;
import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbOptions;

public class BigQueryToMongoDb {

    public interface Options extends PipelineOptions, GcpOptions, MongoDbOptions, BigQueryOptions {

        String getProjectId();

        void setProjectId(String value);
    }

    public static void main(String[] args) throws Exception {

        // Read options from command line.
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        System.out.println("mongoURI *********************" + options.getUri());
        System.out.println("project *********************" + options.getProject());

        TableReference tableSpec =
                new TableReference()
                        .setDatasetId(options.getBigquerydataset())
                        .setTableId(options.getBigquerytable());

        PCollection<TableRow> bigQueryDataset =
                p.apply(BigQueryIO.readTableRows().from(tableSpec));

        bigQueryDataset.apply("bigQueryDataset", MapElements.via(
                new SimpleFunction<TableRow, Document>() {
                    @Override
                    public Document apply(TableRow row) {

                        String id = row.get("id").toString();
                        Object source = row.get("source_data");
                        String timestamp = row.get("timestamp").toString();

                        Document doc = new Document();
                        doc.append("_id", id)
                                .append("source_data", source)
                                .append("timestamp", timestamp);
                        return doc;

                    }
                }
        )).apply(
                MongoDbIO.write()
                        .withUri(options.getUri())
                        .withDatabase(options.getDb())
                        .withCollection(options.getColl())
                       );

        p.run().waitUntilFinish();
    }

}