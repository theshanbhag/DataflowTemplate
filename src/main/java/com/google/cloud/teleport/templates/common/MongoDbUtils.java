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

import java.util.Set;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.Description;
import org.bson.Document;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;



/** Transforms & DoFns & Options for Teleport DatastoreIO. */
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

        void setBigquerydataset(String value);

        @Description("BigQuery Table name to write to")
        String getBigquerytable();

        void setBigquerytable(String value);

    }


    public static TableSchema getTableFieldSchema(String uri, String db, String coll ){
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

        StringBuffer sb = new StringBuffer();
        sb.append(source_data);
        sb.replace(1, 45, "");
        String sourceDataUpdated = sb.toString();
        TableRow row = new TableRow()
                .set("id",document.getObjectId("_id").toString())
                .set("source_data",sourceDataUpdated)
                .set("timestamp", localdate.format(time_format));
        return row;
    }
}
