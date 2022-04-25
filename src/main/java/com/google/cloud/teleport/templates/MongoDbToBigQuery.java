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
import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbReadOptions;
import com.google.cloud.teleport.templates.common.MongoDbUtils.BigQueryOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.transforms.SimpleFunction;
import java.util.ArrayList;
import org.apache.beam.sdk.transforms.MapElements;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.ArrayList;
import java.util.List;
import com.mongodb.util.JSON;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.lang.reflect.Method;
import java.io.Serializable;




import com.google.cloud.teleport.templates.common.BigQueryConverters;


/** Dataflow template which copies Datastore Entities to a BigQuery table. */
public class MongoDbToBigQuery implements Serializable {
  public interface MongoDbToBigQueryOptions
      extends PipelineOptions, MongoDbReadOptions, BigQueryOptions {
    @Description("The BigQuery table spec to write the output to")
      String getProjectId();
      char getScope();

      void setProjectId(String value);
      void setScope(char value);

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
                MongoDbUtils.getTableFieldSchema(options.getScope(), options.getUri(), options.getDb(), options.getColl());

        /** Create BigQuery schema */
        TableReference bigQyertTable = new TableReference();
        bigQyertTable.setProjectId(options.getProjectId());
        bigQyertTable.setDatasetId(options.getBigquerydataset());
        bigQyertTable.setTableId(options.getBigquerytable());
        char scope = options.getScope();

        pipeline
                .apply(
                        MongoDbIO.read().
                                withBucketAuto(true).
                                withUri(options.getUri()).
                                withDatabase(options.getDb()).
                                withCollection(options.getColl())
                ).apply(
                        "Read Documents",
                        MapElements.via(
                                new SimpleFunction<Document, TableRow>() {
                                    @Override
                                    public TableRow apply(Document document) {
                                        return MongoDbUtils.generateTableRow(document, scope);
                                    }
                                }
                        )

                ).apply(
                        BigQueryIO
                                .writeTableRows()
                                .withoutValidation()
                                .to(bigQyertTable)
                                .withSchema(bigquerySchema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                );

        pipeline.run().waitUntilFinish();
    }


//    public static TableRow generateDocumentTableRow(Document document){
//        String source_data = document.toJson();
//        DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//        LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
//        TableRow row = new TableRow()
//                .set("Source_data",source_data)
//                .set("timestamp", localdate.format(time_format));
//        return row;
//    }

//    public static TableRow generateFieldTableRow(Document document){
//        TableRow row = new TableRow();
//        document.forEach((key, value) -> {
//            row.set(key, value.toString());
//        });
//        return row;
//    }
}
