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
package com.google.cloud.teleport.templates.common;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.IOException;
import java.io.Serializable;
import org.bson.Document;


/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class MongoDbUtils implements Serializable{

    /** Options for Reading MongoDb Documents. */
    public interface  MongoDbOptions extends PipelineOptions, DataflowPipelineOptions {

        @Description("MongoDB URI for connecting to MongoDB Cluster")
        @Default.String("mongouri")
        ValueProvider<String> getMongoDbUri();
        void setMongoDbUri(ValueProvider<String> getMongoDbUri);

        @Description("MongoDb Database name to read the data from")
        @Default.String("db")
        ValueProvider<String> getDatabase();
        void setDatabase(ValueProvider<String> database);

        @Description("MongoDb collection to read the data from")
        @Default.String("collection")
        ValueProvider<String> getCollection();
        void setCollection(ValueProvider<String> collection);


    }

    public interface  BigQueryWriteOptions extends PipelineOptions, DataflowPipelineOptions {

        @Description("BigQuery Table name to write to")
        @Default.String("bqtable")
        ValueProvider<String> getOutputTableSpec();
        void setOutputTableSpec(ValueProvider<String> outputTableSpec);

        @Description("BigQuery Table name to write to")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();
        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> bigQueryLoadingTemporaryDirectory);
    }

    public interface  BigQueryReadOptions extends PipelineOptions, DataflowPipelineOptions {

        @Description("BigQuery Table name to write to")
        @Default.String("bqtable")
        ValueProvider<String> getInputTableSpec();
        void setInputTableSpec(ValueProvider<String> inputTableSpec);

        @Description("BigQuery Table name to write to")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();
        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> bigQueryLoadingTemporaryDirectory);
    }


    public static TableSchema getTableFieldSchema(){
        List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();
        bigquerySchemaFields.add(new TableFieldSchema().setName("id").setType("STRING"));
        bigquerySchemaFields.add(new TableFieldSchema().setName("source_data").setType("STRING"));
        bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
        return bigquerySchema;
    }

    public static TableRow generateTableRow(Document document){
        String source_data = document.toJson();
        DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));

        TableRow row = new TableRow()
                .set("id",document.getObjectId("_id").toString())
                .set("source_data",source_data)
                .set("timestamp", localdate.format(time_format));
        return row;
    }

    public static String translateJDBCUrl(String jdbcUrlSecretName) {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            AccessSecretVersionResponse response = client.accessSecretVersion(jdbcUrlSecretName);
            String resp = response.getPayload().getData().toStringUtf8();
            return resp;
        } catch (IOException e) {
            throw new RuntimeException("Unable to read JDBC URL secret");
        }
    }
}
