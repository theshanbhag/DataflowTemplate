//package com.google.cloud.teleport.templates;
//
//import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
//import com.google.cloud.teleport.templates.common.MongoDbUtils;
//import com.google.cloud.teleport.templates.common.MongoDbUtils.MongoDbOptions;
//import com.google.cloud.teleport.templates.common.MongoDbUtils.BigQueryWriteOptions;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
//import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
//import org.apache.beam.sdk.options.Description;
//import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.options.ValueProvider;
//import org.apache.beam.sdk.io.mongodb.MongoDbIO;
//import org.apache.beam.sdk.io.mongodb.MongoDbIO.Read;
//import org.bson.Document;
//import org.apache.beam.sdk.transforms.SimpleFunction;
//import org.apache.beam.sdk.transforms.MapElements;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.api.services.bigquery.model.TableSchema;
//import com.google.api.services.bigquery.model.TableFieldSchema;
//import com.google.api.services.bigquery.model.TableReference;
//import org.apache.beam.sdk.options.ValueProvider;
//import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
//import java.io.Serializable;
//import org.apache.beam.sdk.options.Default;
//
//
//import com.google.api.core.ApiFuture;
//import com.google.cloud.pubsub.v1.Publisher;
//import com.google.protobuf.ByteString;
//import com.google.pubsub.v1.ProjectTopicName;
//import com.google.pubsub.v1.PubsubMessage;
//import com.mongodb.Block;
//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import org.bson.Document;
//import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.io.Serializable;
//import java.util.function.Consumer;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//
//
//
//public class MongoDbToPubSub  implements Serializable {
//
//    public interface MongoDbToPubSubOptions extends PipelineOptions, GcpOptions, MongoDbOptions {
//        @Description("MongoDB URI for connecting to MongoDB Cluster")
//        @Default.String("testtopic")
//        String getPubsubWriteTopic();
//        void setPubsubWriteTopic(String getPubsubWriteTopic);
//
//    }
//
//    public static void main(String[] args) throws Exception {
//        String PROJECT_ID = "gsidemo-246315";
//        String topicId = "Test";
//        String mongoUri = "mongodb+srv://venkatesh:ashwin123@iiotapp.2wqno.mongodb.net";
//
//        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoUri));
//        MongoDatabase database = mongoClient.getDatabase("sample_mflix");
//        MongoCollection<Document> collection = database.getCollection("movies");
//
//
//        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
//        List<ApiFuture<String>> futures = new ArrayList<>();
//        final Publisher publisher = Publisher.newBuilder(topicName).build();
//
//        MongoDbToPubSubOptions options = PipelineOptionsFactory
//                .fromArgs(args)
//                .withValidation()
//                .as(MongoDbToPubSubOptions.class);
//        options.setStreaming(true);
//
//
//        String mongoDbURI = MongoDbUtils.translateJDBCUrl(options.getMongoDbUri().get());
//        String db = MongoDbUtils.translateJDBCUrl(options.getDatabase().get());
//        String coll = MongoDbUtils.translateJDBCUrl(options.getCollection().get());
//
//
//        TableSchema bigquerySchema =
//                MongoDbUtils.getTableFieldSchema(mongoDbURI, db, coll);
//
//        Pipeline pipeline = Pipeline.create(options);
//
//
//                .apply(
//                        PubsubIO.writeStrings().to(options.getPubsubWriteTopic())
//                );
//        pipeline.run().waitUntilFinish();
//    }
//}