/*
 * Copyright (C) 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates;
import com.google.cloud.teleport.templates.common.MongoDbUtils;
import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbOptions;
import com.google.cloud.teleport.templates.common.MongoDbUtils.BigQueryOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
import org.bson.Document;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.MapElements;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;


/** Dataflow template which copies MongoDb document to a BigQuery table. */
public class MongoDbToBigQuery {
  public interface MongoDbToBigQueryOptions
      extends PipelineOptions, MongoDbOptions, BigQueryOptions {
    @Description("The BigQuery table spec to write the output to")
      String getProjectId();

      void setProjectId(String value);
  }

  /**
   * Runs a pipeline which reads in Documents from MongoDB
   * a returns JSON string that conforms to the BigQuery TableRow spec and writes the
   * TableRows to BigQuery.
   *
   * @param args arguments to the pipeline
   */
    public static void main(String[] args) {
        MongoDbToBigQueryOptions options =
                  PipelineOptionsFactory.fromArgs(args).withValidation().as(MongoDbToBigQueryOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        TableSchema bigquerySchema =
                MongoDbUtils.getTableFieldSchema(options.getUri(), options.getDb(), options.getColl());

        /** Create BigQuery schema */
        TableReference bigQyertTable = new TableReference();
        bigQyertTable.setProjectId(options.getProjectId());
        bigQyertTable.setDatasetId(options.getBigquerydataset());
        bigQyertTable.setTableId(options.getBigquerytable());

        pipeline
                .apply(
                        MongoDbIO.read().
                                withUri(options.getUri()).
                                withBucketAuto(true).
                                withDatabase(options.getDb()).
                                withCollection(options.getColl())
                ).apply(
                        "Read Documents", MapElements.via(
                                new SimpleFunction<Document, TableRow>() {
                                    @Override
                                    public TableRow apply(Document document) {
                                        return MongoDbUtils.generateTableRow(document);
                                    }
                                }
                        )

                ).apply(
                        BigQueryIO.writeTableRows()
                        .to(bigQyertTable)
                        .withSchema(bigquerySchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                );

        pipeline.run().waitUntilFinish();
    }
}
