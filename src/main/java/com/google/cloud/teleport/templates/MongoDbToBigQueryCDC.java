///*
// * Copyright (C) 2018 Google LLC
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.google.cloud.teleport.templates;
//
//import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
//
//import com.google.cloud.teleport.templates.common.MongoDbUtils;
//import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbOptions;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//import org.apache.beam.sdk.options.*;
//import org.apache.beam.sdk.transforms.MapElements;
//import org.apache.beam.sdk.transforms.SimpleFunction;
//import org.bson.Document;
//import com.google.api.services.bigquery.model.TableSchema;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.io.Serializable;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//
//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.google.protobuf.ByteString;
//import com.google.pubsub.v1.ProjectTopicName;
//import com.google.pubsub.v1.PubsubMessage;
//import com.mongodb.Block;
//import com.google.api.core.ApiFuture;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.google.cloud.pubsub.v1.Publisher;
//import java.util.ArrayList;
//import java.util.List;
//
//
//
//public class MongoDbToBigQueryCDC implements Serializable {
//
//    public interface PubSubToBigQuery extends PipelineOptions, GcpOptions, MongoDbOptions {
//
//        @Description("The Cloud Pub/Sub topic to read from.")
//        ValueProvider<String> getInputTopic();
//        void setInputTopic(ValueProvider<String> value);
//
//        String getProjectId();
//        void setProjectId(String value);
//
//        @Description("BigQuery Dataset Id to write to")
//        ValueProvider<String> getOutputTableSpec();
//        void setOutputTableSpec(ValueProvider<String> value);
//
//    }
//
//    public static void main(String[] args) throws IOException {
//        PubSubToBigQuery options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
//                .as(PubSubToBigQuery.class);
//        options.setStreaming(true);
//
//        String mongoDbURI = MongoDbUtils.translateJDBCUrl(options.getMongoDbUri().get());
//        String database = MongoDbUtils.translateJDBCUrl(options.getDatabase().get());
//        String collection = MongoDbUtils.translateJDBCUrl(options.getCollection().get());
//
//        TableSchema bigquerySchema =
//                MongoDbUtils.getTableFieldSchema(mongoDbURI, database, collection);
//
//        Pipeline pipeline = Pipeline.create(options);
//
//
//
//
//
//
//        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoDbURI));
//        MongoDatabase db = mongoClient.getDatabase("sample_mflix");
//        MongoCollection<Document> coll = db.getCollection("movies");
//
//        ProjectTopicName topicName = ProjectTopicName.of(options.getProject(), "Test");
//        List<ApiFuture<String>> futures = new ArrayList<>();
//        final Publisher publisher = Publisher.newBuilder(topicName).build();
//
//        Block<ChangeStreamDocument<Document>> printBlock = new Block<ChangeStreamDocument<Document>>() {
//            @Override
//            public void apply(final ChangeStreamDocument<Document> changeStreamDocument) {
//                System.out.println(changeStreamDocument.getFullDocument().toJson());
//
//
//                try {
//                    System.out.println(changeStreamDocument.getFullDocument());
//                    ByteString data = ByteString.copyFromUtf8(changeStreamDocument.getFullDocument().toJson());
//                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
//                            .setData(data)
//                            .build();
//                    // Schedule a message to be published. Messages are automatically batched.
//                    ApiFuture<String> future = publisher.publish(pubsubMessage);
//                    futures.add(future);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                } finally {
//
//                }
//            }
//        };
//
//        coll.watch().forEach(printBlock);
//
//
//
//
//
//        pipeline
//                .apply(
//                        "Read PubSub Messages",
//                        PubsubIO.
//                                readStrings().
//                                fromTopic(options.getInputTopic())
//                )
//                .apply(
//                        "Read and transform Movies data",
//                        MapElements.via(
//                                new SimpleFunction<String, TableRow>() {
//                                    @Override
//                                    public TableRow apply(String document) {
//                                        Gson gson = new GsonBuilder().create();
//                                        HashMap<String, Object> parsedMap = gson.fromJson(document, HashMap.class);
//
//                                        DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//                                        LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
//                                        TableRow row =
//                                                new TableRow()
//                                                        .set("source_data", parsedMap.toString())
//                                                        .set("timestamp", localdate.format(time_format));
//                                        return row;
//                                    }
//                                }
//                        )
//                )
//                .apply(
//                        BigQueryIO
//                                .writeTableRows()
//                                .to(options.getOutputTableSpec())
//                                .withSchema(bigquerySchema)
//                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                );
//        pipeline.run();
//    }
//}
//
