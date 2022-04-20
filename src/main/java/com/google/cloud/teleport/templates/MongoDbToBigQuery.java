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
import com.google.cloud.teleport.templates.common.MongoDbConverters.MongoDbReadOptions;
import com.google.cloud.teleport.templates.common.MongoDbConverters.ReadJsonEntities;
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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

import com.google.cloud.teleport.templates.common.BigQueryConverters;


/** Dataflow template which copies Datastore Entities to a BigQuery table. */
public class MongoDbToBigQuery {
  public interface MongoDbToBigQueryOptions
      extends PipelineOptions {
    @Description("The BigQuery table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);
  }

  /**
   * Runs a pipeline which reads in Entities from MongoDB, passes in the JSON encoded Entities to
   * a returns JSON that conforms to the BigQuery TableRow spec and writes the
   * TableRows to BigQuery.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    MongoClient mongoClient = MongoClients.create(options.getUri());
    MongoDatabase database = mongoClient.getDatabase(options.getDb());
    MongoCollection<Document> collection = database.getCollection(options.getColl());
    Document doc = collection.find().first();

    List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();


    /*  */
//    doc.forEach((key, value) -> {
//        if(value.getClass().getName()=="java.lang.String"){
//            bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("STRING"));
//        }else if(value.getClass().getName()=="java.lang.Integer"){
//            bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("INT64"));
//        }else {
//            bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("STRING"));
//        }
//        System.out.println(">>>>>>>>>>>>>>>>>"+value.getClass().getName());
//    });
//    bigquerySchemaFields.add(new TableFieldSchema().setName("src_data").setType("STRING"));
//    TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
//    /** For schema creation */

    TableReference table1 = new TableReference();
    table1.setProjectId(options.getProjectId());
    table1.setDatasetId(options.getBigquerydataset());
    table1.setTableId(options.getBigquerytable());


//    pipeline
//            .apply(
//                  ReadJsonEntities.newBuilder()
//                  .setUri(options.getUri())
//                  .setDb(options.getDb())
//                  .setColl(options.getColl()).build())
//            .apply(
//                    "Read Documents", MapElements.via(
//                            new SimpleFunction<Document, TableRow>() {
//                                @Override
//                                public TableRow apply(Document document) {
////                                    TableRow row = new TableRow();
////                                    document.forEach((key, value) -> {
////                                            row.set(key, value.toString());
////                                    });
////                                    return row;
//                                    TableRow row = new TableRow();
//                                    String source_data = document.toJson();
//                                    row.set("src_data",source_data );
//                                    return row;
//                                }
//                            }
//                    )
//            ).apply(BigQueryIO.writeTableRows()
//                            .to(table1)
//                            .withSchema(bigquerySchema)
//                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
//            );
      pipeline
            .apply(
                  ReadJsonEntities.newBuilder()
                  .setUri(options.getUri())
                  .setDb(options.getDb())
                  .setColl(options.getColl()).build())
            .apply(
                    "Read Documents", MapElements.via(
                            new SimpleFunction<Document, String>() {
                                @Override
                                public String apply(Document document) {
                                    String jsonDoc = document.toJson();
                                    return jsonDoc;
                                }
                            })
            )
            .apply(
                BigQueryConverters.jsonToTableRow()
            )
              .apply(
                      "WriteBigQuery",
                      BigQueryIO.writeTableRows()
                              .to(bqtable)
                              .withSchema(bigquerySchema)
                              .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                              .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                              .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));
    pipeline.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions {

        @Description("Table to write to, specified as " + "<project_id>:<dataset_id>.<table_id>")
        @Validation.Required
        String getUri();
        String getDb();
        String getColl();
        String getProjectId();
        String getBigquerydataset();
        String getBigquerytable();
        String getOutputTableSpec();
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setUri(String value);
        void setDb(String value);
        void setColl(String value);
        void setProjectId(String value);
        void setBigquerydataset(String value);
        void setBigquerytable(String value);
        void setOutputTableSpec(String value);
        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> value);
  }
}
