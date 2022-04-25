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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorMessage;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Set;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.bson.Document;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.SimpleFunction;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.beam.sdk.transforms.MapElements;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;



import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

public class MongoDbUtils {

    /** Options for Reading MongoDb Documents. */
    public interface  MongoDbReadOptions extends PipelineOptions {

        @Description("MongoDB URI for connecting to MongoDB Cluster")
        String getUri();

        void setUri(String value);

        @Description("MongoDb Database name to read the data from")
        String getDb();

        void setDb(String value);

        @Description("MongoDb collection to read the data from")
        String getColl();

        void setColl(String value);

    }

    public interface  BigQueryOptions extends PipelineOptions {

        @Description("BigQuery Dataset Id to write to")
        String getBigquerydataset();

        @Description("BigQuery Table name to write to")
        String getBigquerytable();

        void setBigquerydataset(String value);

        void setBigquerytable(String value);

    }


    public static TableSchema getTableFieldSchema(char scope, String uri, String db, String coll ){
        List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();
        if(scope == '1'){
            bigquerySchemaFields.add(new TableFieldSchema().setName("Source_data").setType("STRING"));
            bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        }else if(scope == '2'){
            MongoClient mongoClient = MongoClients.create(uri);
            MongoDatabase database = mongoClient.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(coll);
            Document doc = collection.find().first();
            doc.forEach((key, value) ->
                {
                    if(value.getClass().getName()=="java.lang.String")
                    {
                        bigquerySchemaFields.add(
                                new TableFieldSchema()
                                        .setName(key)
                                        .setType("STRING")
                        );
                    }
                    else if(value.getClass().getName()=="java.lang.Integer")
                    {
                        bigquerySchemaFields.add(
                                new TableFieldSchema()
                                        .setName(key)
                                        .setType("INT64")
                        );
                    }
                    else if(value.getClass().getName()=="java.lang.Long")
                    {
                        bigquerySchemaFields.add(
                                new TableFieldSchema()
                                        .setName(key)
                                        .setType("FLOAT")
                        );
                    }else if(value.getClass().getName()=="java.util.Date")
                    {
                        bigquerySchemaFields.add(
                                new TableFieldSchema()
                                        .setName(key)
                                        .setType("DATE")
                        );
                    }
                    else
                    {
                        bigquerySchemaFields.add(
                                new TableFieldSchema()
                                        .setName(key)
                                        .setType("STRING")
                        );
                    }
                    System.out.println(">>>>>>>>> " + value.getClass().getName());
                }
            );
        }

        TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
        return bigquerySchema;
    }

    public static TableRow generateDocumentTableRow(Document document){
        String source_data = document.toJson();
        DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
        TableRow row = new TableRow()
                .set("Source_data",source_data)
                .set("timestamp", localdate.format(time_format));
        return row;
    }

    public static TableRow generateFieldTableRow(Document document){
        TableRow row = new TableRow();
        document.forEach((key, value) -> {
            row.set(key, value.toString());
        });
        return row;
    }

    public static TableRow generateTableRow(Document document, char scope){

        if(scope == '1'){
            String source_data = document.toJson();
            DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
            TableRow row = new TableRow()
                    .set("Source_data",source_data)
                    .set("timestamp", localdate.format(time_format));
            return row;
        }else {
            TableRow row = new TableRow();
            document.forEach((key, value) -> {
                if(value.getClass().getName()=="java.util.Date"){
                    row.set(key, value.toString());
                }else{
                    row.set(key, value.toString());
                }
            });
            return row;
        }
    }
}
