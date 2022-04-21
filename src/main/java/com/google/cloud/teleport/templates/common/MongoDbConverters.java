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
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

/** Transforms & DoFns & Options for Teleport DatastoreIO. */
public class MongoDbConverters {

    /** Options for Reading MongoDb Documents. */
    public interface  MongoDbReadOptions extends PipelineOptions {

        @Description("MongoDB URI for connecting to MongoDB Cluster")
        String getUri();

        @Description("MongoDb Database name to read the data from")
        String getDb();

        @Description("MongoDb collection to read the data from")
        String getColl();

        void setUri(String value);

        void setDb(String value);

        void setColl(String value);

        void setProjectId(String value);

    }

    @AutoValue
    public abstract static class ReadJsonEntities extends PTransform<PBegin, PCollection<Document>> {

        public abstract String uri();

        public abstract String db();

        public abstract String coll();

        /** Builder for ReadJsonEntities. */
        @AutoValue.Builder
        public abstract static class Builder {

            public abstract Builder setUri(String uri);

            public abstract Builder setDb(String db);

            public abstract Builder setColl(String coll);

            public abstract ReadJsonEntities build();
        }

        public static Builder newBuilder() {
            return new AutoValue_MongoDbConverters_ReadJsonEntities.Builder();
        }

        @Override
        public PCollection<Document> expand(PBegin begin) {
            return begin
                    .apply(
                            "ReadFromMongoDb",
                            MongoDbIO.read().
                            withUri(uri()).
                            withBucketAuto(true).
                            withDatabase(db()).
                            withCollection(coll()));
        }


    }

    public static TableSchema getTableFieldSchema(char version, String uri, String db, String coll ){
        List<TableFieldSchema> bigquerySchemaFields = new ArrayList<>();
        if(version == '1'){
            bigquerySchemaFields.add(new TableFieldSchema().setName("Source_data").setType("STRING"));
            bigquerySchemaFields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
        }else if(version == '2'){
            MongoClient mongoClient = MongoClients.create(uri);
            MongoDatabase database = mongoClient.getDatabase(db);
            MongoCollection<Document> collection = database.getCollection(coll);
            Document doc = collection.find().first();
            doc.forEach((key, value) ->
                {
                    if(value.getClass().getName()=="java.lang.String"){
                        bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("STRING"));
                    }else if(value.getClass().getName()=="java.lang.Integer"){
                        bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("INT64"));
                    }else if(value.getClass().getName()=="java.lang.Long"){
                        bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("FLOAT"));
                    }else {
                        bigquerySchemaFields.add(new TableFieldSchema().setName(key).setType("STRING"));
                    }
                }
            );
        }

        TableSchema bigquerySchema = new TableSchema().setFields(bigquerySchemaFields);
        return bigquerySchema;
    }

//    public class BuildRowFromDocument {
//        @Override
//        public TableRow apply(Document document) {
////                                    TableRow row = new TableRow();
////                                    document.forEach((key, value) -> {
////                                            row.set(key, value.toString());
////                                    });
////                                    return row;
//            String source_data = document.toJson();
//            DateTimeFormatter time_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//            LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
//            TableRow row = new TableRow()
//                    .set("Source_data",source_data)
//                    .set("timestamp", localdate.format(time_format));
//            return row;
//        }
//    }

    /** Writes Entities encoded in JSON to Datastore. */
//    @AutoValue
//    public abstract static class WriteJsonEntities
//            extends PTransform<PCollection<String>, PCollectionTuple> {
//        public abstract ValueProvider<String> projectId();
//
//        public abstract ValueProvider<Integer> hintNumWorkers();
//
//        public abstract Boolean throttleRampup();
//
//        public abstract TupleTag<String> errorTag();
//
//        /** Builder for WriteJsonEntities. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setProjectId(ValueProvider<String> projectId);
//
//            public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);
//
//            public abstract Builder setThrottleRampup(Boolean throttleRampup);
//
//            public abstract Builder setErrorTag(TupleTag<String> errorTag);
//
//            public abstract WriteJsonEntities build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_WriteJsonEntities.Builder()
//                    .setHintNumWorkers(StaticValueProvider.of(500))
//                    .setThrottleRampup(true); // defaults
//        }
//
//        @Override
//        public PCollectionTuple expand(PCollection<String> entityJson) {
//            TupleTag<Entity> goodTag = new TupleTag<>();
//            DatastoreV1.Write datastoreWrite =
//                    DatastoreIO.v1().write().withProjectId(projectId()).withHintNumWorkers(hintNumWorkers());
//            if (!throttleRampup()) {
//                datastoreWrite = datastoreWrite.withRampupThrottlingDisabled();
//            }
//
//            PCollectionTuple entities =
//                    entityJson
//                            .apply("StringToEntity", ParDo.of(new JsonToEntity()))
//                            .apply(
//                                    "CheckSameKey",
//                                    CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());
//
//            entities.get(goodTag).apply("WriteToDatastore", datastoreWrite);
//            return entities;
//        }
//    }

    /** Removes any entities that have the same key, and throws exception. */
//    @AutoValue
//    public abstract static class CheckSameKey
//            extends PTransform<PCollection<Entity>, PCollectionTuple> {
//        public abstract TupleTag<Entity> goodTag();
//
//        public abstract TupleTag<String> errorTag();
//
//        private Counter duplicatedKeys = Metrics.counter(CheckSameKey.class, "duplicated-keys");
//
//        /** Builder for {@link CheckSameKey}. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setGoodTag(TupleTag<Entity> goodTag);
//
//            public abstract Builder setErrorTag(TupleTag<String> errorTag);
//
//            public abstract CheckSameKey build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_CheckSameKey.Builder();
//        }
//
//        /** Handles detecting entities that have duplicate keys that are non equal. */
////        @Override
////        public PCollectionTuple expand(PCollection<Entity> entities) {
////            return entities
////                    .apply(
////                            "ExposeKeys",
////                            ParDo.of(
////                                    new DoFn<Entity, KV<byte[], Entity>>() {
////                                        @ProcessElement
////                                        public void processElement(ProcessContext c) throws IOException {
////                                            Entity e = c.element();
////
////                                            // Serialize Key deterministically
////                                            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
////                                            CodedOutputStream output = CodedOutputStream.newInstance(byteOutputStream);
////                                            output.useDeterministicSerialization();
////                                            c.element().getKey().writeTo(output);
////                                            output.flush();
////
////                                            c.output(KV.of(byteOutputStream.toByteArray(), e));
////                                        }
////                                    }))
////                    .apply(GroupByKey.create())
////                    .apply(
////                            "ErrorOnDuplicateKeys",
////                            ParDo.of(
////                                            new DoFn<KV<byte[], Iterable<Entity>>, Entity>() {
////                                                private EntityJsonPrinter entityJsonPrinter;
////
////                                                @Setup
////                                                public void setup() {
////                                                    entityJsonPrinter = new EntityJsonPrinter();
////                                                }
////
////                                                @ProcessElement
////                                                public void processElement(ProcessContext c) throws IOException {
////                                                    Iterator<Entity> entities = c.element().getValue().iterator();
////                                                    Entity entity = entities.next();
////                                                    if (entities.hasNext()) {
////                                                        do {
////                                                            duplicatedKeys.inc();
////                                                            ErrorMessage errorMessage =
////                                                                    ErrorMessage.newBuilder()
////                                                                            .setMessage("Duplicate Datastore Key")
////                                                                            .setData(entityJsonPrinter.print(entity))
////                                                                            .build();
////                                                            c.output(errorTag(), errorMessage.toJson());
////                                                            entity = entities.hasNext() ? entities.next() : null;
////                                                        } while (entity != null);
////                                                    } else {
////                                                        c.output(entity);
////                                                    }
////                                                }
////                                            })
////                                    .withOutputTags(goodTag(), TupleTagList.of(errorTag())));
////        }
//    }

    /** Deletes matching entities. */
//    @AutoValue
//    public abstract static class DatastoreDeleteEntityJson
//            extends PTransform<PCollection<String>, PDone> {
//        public abstract ValueProvider<String> projectId();
//
//        public abstract ValueProvider<Integer> hintNumWorkers();
//
//        public abstract Boolean throttleRampup();
//
//        /** Builder for DatastoreDeleteEntityJson. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setProjectId(ValueProvider<String> projectId);
//
//            public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);
//
//            public abstract Builder setThrottleRampup(Boolean throttleRampup);
//
//            public abstract DatastoreDeleteEntityJson build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_DatastoreDeleteEntityJson.Builder()
//                    .setHintNumWorkers(StaticValueProvider.of(500))
//                    .setThrottleRampup(true); // defaults
//        }
//
//        @Override
//        public PDone expand(PCollection<String> entityJson) {
//            DatastoreV1.DeleteKey datastoreDelete =
//                    DatastoreIO.v1()
//                            .deleteKey()
//                            .withProjectId(projectId())
//                            .withHintNumWorkers(hintNumWorkers());
//            if (!throttleRampup()) {
//                datastoreDelete = datastoreDelete.withRampupThrottlingDisabled();
//            }
//            return entityJson
//                    .apply("StringToKey", ParDo.of(new JsonToKey()))
//                    .apply("DeleteKeys", datastoreDelete);
//        }
//    }

    /** Gets all the unique datastore schemas in collection. */
//    @AutoValue
//    public abstract static class DatastoreReadSchemaCount
//            extends PTransform<PBegin, PCollection<String>> {
//        public abstract ValueProvider<String> projectId();
//
//        public abstract ValueProvider<String> gqlQuery();
//
//        public abstract ValueProvider<String> namespace();
//
//        /** Builder for DatastoreReadSchemaCount. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setGqlQuery(ValueProvider<String> gqlQuery);
//
//            public abstract Builder setProjectId(ValueProvider<String> projectId);
//
//            public abstract Builder setNamespace(ValueProvider<String> namespace);
//
//            public abstract DatastoreReadSchemaCount build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_DatastoreReadSchemaCount.Builder();
//        }
//
//        @Override
//        public PCollection<String> expand(PBegin begin) {
//            return begin
//                    .apply(
//                            "ReadFromDatastore",
//                            DatastoreIO.v1()
//                                    .read()
//                                    .withProjectId(projectId())
//                                    .withLiteralGqlQuery(gqlQuery())
//                                    .withNamespace(namespace()))
//                    .apply("ParseEntitySchema", ParDo.of(new EntityToSchemaJson()))
//                    .apply("CountUniqueSchemas", Count.<String>perElement())
//                    .apply(
//                            "Jsonify",
//                            ParDo.of(
//                                    new DoFn<KV<String, Long>, String>() {
//                                        @ProcessElement
//                                        public void processElement(ProcessContext c) {
//                                            JsonObject out = new JsonObject();
//                                            out.addProperty("schema", c.element().getKey());
//                                            out.addProperty("count", c.element().getValue());
//                                            c.output(out.toString());
//                                        }
//                                    }));
//        }
//    }

    /**
     * DoFn for converting a Datastore Entity to JSON. Json in mapped using protov3:
     * https://developers.google.com/protocol-buffers/docs/proto3#json
     */
//    public static class EntityToJson extends DoFn<Entity, String> {
//        private EntityJsonPrinter entityJsonPrinter;
//
//        @Setup
//        public void setup() {
//            entityJsonPrinter = new EntityJsonPrinter();
//        }
//
//        /** Processes Datstore entity into json. */
//        @ProcessElement
//        public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
//            Entity entity = c.element();
//            String entityJson = entityJsonPrinter.print(entity);
//            c.output(entityJson);
//        }
//    }

    /**
     * DoFn for converting a Protov3 JSON Encoded Entity to a Datastore Entity. JSON in mapped
     * protov3: https://developers.google.com/protocol-buffers/docs/proto3#json
     */
//    public static class JsonToEntity extends DoFn<String, Entity> {
//        private EntityJsonParser entityJsonParser;
//
//        @Setup
//        public void setup() {
//            entityJsonParser = new EntityJsonParser();
//        }
//
//        @ProcessElement
//        public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
//            String entityJson = c.element();
//            Entity.Builder entityBuilder = Entity.newBuilder();
//            entityJsonParser.merge(entityJson, entityBuilder);
//
//            // Build entity who's key has an empty project Id.
//            // This allows DatastoreIO to handle what project Entities are loaded into
//            com.google.datastore.v1.Key k = entityBuilder.build().getKey();
//            entityBuilder.setKey(
//                    com.google.datastore.v1.Key.newBuilder()
//                            .addAllPath(k.getPathList())
//                            .setPartitionId(
//                                    PartitionId.newBuilder()
//                                            .setProjectId("")
//                                            .setNamespaceId(k.getPartitionId().getNamespaceId())));
//
//            c.output(entityBuilder.build());
//        }
//    }

    /**
     * DoFn for converting a Protov3 Json Encoded Entity and extracting its key. Expects json in
     * format of: { key: { "partitionId": {"projectId": "", "namespace": ""}, "path": [ {"kind": "",
     * "id": "", "name": ""}]}
     */
//    public static class JsonToKey extends DoFn<String, Key> {
//        private EntityJsonParser entityJsonParser;
//
//        @Setup
//        public void setup() {
//            entityJsonParser = new EntityJsonParser();
//        }
//
//        @ProcessElement
//        public void processElement(ProcessContext c) throws InvalidProtocolBufferException {
//            String entityJson = c.element();
//            Entity e = entityJsonParser.parse(entityJson);
//            c.output(e.getKey());
//        }
//    }

    /** DoFn for extracting the Schema of a Entity. */
//    public static class EntityToSchemaJson extends DoFn<Entity, String> {
//
//        /**
//         * Grabs the schema for what data is in the Array.
//         *
//         * @param arrayValue a populated array
//         * @return a schema of what kind of data is in the array
//         */
//        private JsonArray arraySchema(ArrayValue arrayValue) {
//            Set<JsonObject> entities = new HashSet<>();
//            Set<String> primitives = new HashSet<>();
//            Set<JsonArray> subArrays = new HashSet<>();
//
//            arrayValue.getValuesList().stream()
//                    .forEach(
//                            value -> {
//                                switch (value.getValueTypeCase()) {
//                                    case ENTITY_VALUE:
//                                        entities.add(entitySchema(value.getEntityValue()));
//                                        break;
//                                    case ARRAY_VALUE:
//                                        subArrays.add(arraySchema(value.getArrayValue()));
//                                        break;
//                                    default:
//                                        primitives.add(value.getValueTypeCase().toString());
//                                        break;
//                                }
//                            });
//
//            JsonArray jsonArray = new JsonArray();
//            entities.stream().forEach(jsonArray::add);
//            primitives.stream().forEach(jsonArray::add);
//            subArrays.stream().forEach(jsonArray::add);
//            return jsonArray;
//        }
//
//        /**
//         * Grabs the schema for what data is in the Entity.
//         *
//         * @param entity a populated entity
//         * @return a schema of what kind of data is in the entity
//         */
//        private JsonObject entitySchema(Entity entity) {
//            JsonObject jsonObject = new JsonObject();
//            entity.getPropertiesMap().entrySet().stream()
//                    .forEach(
//                            entrySet -> {
//                                String key = entrySet.getKey();
//                                Value value = entrySet.getValue();
//                                switch (value.getValueTypeCase()) {
//                                    case ENTITY_VALUE:
//                                        jsonObject.add(key, entitySchema(value.getEntityValue()));
//                                        break;
//                                    case ARRAY_VALUE:
//                                        jsonObject.add(key, arraySchema(value.getArrayValue()));
//                                        break;
//                                    default:
//                                        jsonObject.addProperty(key, value.getValueTypeCase().toString());
//                                }
//                            });
//            return jsonObject;
//        }
//
//        @ProcessElement
//        public void processElement(ProcessContext c) throws IOException {
//            Entity entity = c.element();
//            JsonObject schema = entitySchema(entity);
//            c.output(schema.toString());
//        }
//    }

    /** Converts an Entity to a JSON String. */
//    public static class EntityJsonPrinter {
//
//        // A cached jsonPrinter
//        private JsonFormat.Printer jsonPrinter;
//
//        public EntityJsonPrinter() {
//            TypeRegistry typeRegistry = TypeRegistry.newBuilder().add(Entity.getDescriptor()).build();
//            jsonPrinter =
//                    JsonFormat.printer().usingTypeRegistry(typeRegistry).omittingInsignificantWhitespace();
//        }
//
//        /**
//         * Prints an Entity as a JSON String.
//         *
//         * @param entity a Datastore Protobuf Entity.
//         * @return Datastore Entity encoded as a JSON String.
//         * @throws InvalidProtocolBufferException
//         */
//        public String print(Entity entity) throws InvalidProtocolBufferException {
//            return jsonPrinter.print(entity);
//        }
//    }

    /** Converts a JSON String to an Entity. */
//    public static class EntityJsonParser {
//
//        // A cached jsonParser
//        private JsonFormat.Parser jsonParser;
//
//        public EntityJsonParser() {
//            TypeRegistry typeRegistry = TypeRegistry.newBuilder().add(Entity.getDescriptor()).build();
//
//            jsonParser = JsonFormat.parser().usingTypeRegistry(typeRegistry);
//        }
//
//        public void merge(String json, Entity.Builder entityBuilder)
//                throws InvalidProtocolBufferException {
//            jsonParser.merge(json, entityBuilder);
//        }
//
//        public Entity parse(String json) throws InvalidProtocolBufferException {
//            Entity.Builder entityBuilter = Entity.newBuilder();
//            merge(json, entityBuilter);
//            return entityBuilter.build();
//        }
//    }

    /** Writes Entities to Datastore. */
//    @AutoValue
//    public abstract static class WriteEntities
//            extends PTransform<PCollection<Entity>, PCollectionTuple> {
//        public abstract ValueProvider<String> projectId();
//
//        public abstract ValueProvider<Integer> hintNumWorkers();
//
//        public abstract Boolean throttleRampup();
//
//        public abstract TupleTag<String> errorTag();
//
//        /** Builder for WriteJsonEntities. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setProjectId(ValueProvider<String> projectId);
//
//            public abstract Builder setHintNumWorkers(ValueProvider<Integer> hintNumWorkers);
//
//            public abstract Builder setThrottleRampup(Boolean throttleRampup);
//
//            public abstract Builder setErrorTag(TupleTag<String> errorTag);
//
//            public abstract WriteEntities build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_WriteEntities.Builder()
//                    .setHintNumWorkers(StaticValueProvider.of(500))
//                    .setThrottleRampup(true); // defaults
//        }
//
//        @Override
//        public PCollectionTuple expand(PCollection<Entity> entity) {
//            TupleTag<Entity> goodTag = new TupleTag<>();
//            DatastoreV1.Write datastoreWrite =
//                    DatastoreIO.v1().write().withProjectId(projectId()).withHintNumWorkers(hintNumWorkers());
//            if (!throttleRampup()) {
//                datastoreWrite = datastoreWrite.withRampupThrottlingDisabled();
//            }
//
//            // Due to the fact that DatastoreIO does non-transactional writing to Datastore, writing the
//            // same entity more than once in the same commit is not supported (error "A non-transactional
//            // commit may not contain multiple mutations affecting the same entity). Messages with the
//            // same key are thus not written to Datastore and instead routed to an error PCollection for
//            // further handlig downstream.
//            PCollectionTuple entities =
//                    entity.apply(
//                            "CheckSameKey",
//                            CheckSameKey.newBuilder().setErrorTag(errorTag()).setGoodTag(goodTag).build());
//            entities.get(goodTag).apply("WriteToDatastore", datastoreWrite);
//            return entities;
//        }
//    }

    /** Removes any invalid entities that don't have a key. */
//    @AutoValue
//    public abstract static class CheckNoKey
//            extends PTransform<PCollection<Entity>, PCollectionTuple> {
//
//        abstract TupleTag<Entity> successTag();
//
//        abstract TupleTag<String> failureTag();
//
//        /** Builder for {@link CheckNoKey}. */
//        @AutoValue.Builder
//        public abstract static class Builder {
//            public abstract Builder setSuccessTag(TupleTag<Entity> successTag);
//
//            public abstract Builder setFailureTag(TupleTag<String> failureTag);
//
//            public abstract CheckNoKey build();
//        }
//
//        public static Builder newBuilder() {
//            return new AutoValue_DatastoreConverters_CheckNoKey.Builder();
//        }
//
//        @Override
//        public PCollectionTuple expand(PCollection<Entity> entities) {
//            return entities.apply(
//                    "DetectInvalidEntities",
//                    ParDo.of(
//                                    new DoFn<Entity, Entity>() {
//                                        private EntityJsonPrinter entityJsonPrinter;
//
//                                        @Setup
//                                        public void setup() {
//                                            entityJsonPrinter = new EntityJsonPrinter();
//                                        }
//
//                                        @ProcessElement
//                                        public void processElement(ProcessContext c) throws IOException {
//                                            Entity entity = c.element();
//                                            if (entity.hasKey()) {
//                                                c.output(entity);
//                                            } else {
//                                                ErrorMessage errorMessage =
//                                                        ErrorMessage.newBuilder()
//                                                                .setMessage("Datastore Entity Without Key")
//                                                                .setData(entityJsonPrinter.print(entity))
//                                                                .build();
//                                                c.output(failureTag(), errorMessage.toJson());
//                                            }
//                                        }
//                                    })
//                            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
//        }
//    }
}
