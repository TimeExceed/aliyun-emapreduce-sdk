/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.tablestore.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Objects;
import java.util.Iterator;
import java.io.Externalizable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import scala.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.aliyun.openservices.tablestore.hadoop.Credential;
import com.aliyun.openservices.tablestore.hadoop.Endpoint;
import com.aliyun.openservices.tablestore.hadoop.PrimaryKeyWritable;
import com.aliyun.openservices.tablestore.hadoop.RowWritable;
import com.aliyun.openservices.tablestore.hadoop.BatchWriteWritable;
import com.aliyun.openservices.tablestore.hadoop.TableStore;
import com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat;
import com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;

public class CreateReadWrite {
    private static String endpoint = "YourEndpoint";
    private static String accessKeyId = "YourAccessKeyId";
    private static String accessKeySecret = "YourAccessKeySecret";
    private static String petTable = "pet";
    private static String ownerTable = "copypet";

    private static class SeriableText implements Writable, Externalizable {
        private String text;

        public SeriableText() {
        }

        SeriableText(String text) {
            this.text = text;
        }

        @Override public void write(DataOutput out) throws IOException {
            out.writeUTF(text);
        }

        @Override public void readFields(DataInput in) throws IOException {
            this.text = in.readUTF();
        }

        @Override public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
            this.readFields(in);
        }

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            this.write(out);
        }

    }
    
    private static SyncClientInterface getOTSClient() {
        Endpoint ep = new Endpoint(endpoint);
        return new SyncClient(ep.endpoint, accessKeyId,
            accessKeySecret, ep.instance);
    }

    private static TableMeta fetchPetTableMeta(SyncClientInterface ots) {
        DescribeTableResponse resp = ots.describeTable(
            new DescribeTableRequest(petTable));
        return resp.getTableMeta();
    }

    private static RangeRowQueryCriteria newCriteria(String table, TableMeta meta) {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(table);
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    private static void createOwnerTable(SyncClientInterface ots) {
        TableMeta meta = new TableMeta(ownerTable);
        meta.addPrimaryKeyColumn("owner", PrimaryKeyType.STRING);
        TableOptions opts = new TableOptions();
        opts.setTimeToLive(-1);
        opts.setMaxVersions(1);
        opts.setMaxTimeDeviation(3600);
        CreateTableRequest creq = new CreateTableRequest(meta, opts);
        try {
            ots.createTable(creq);
        } catch (TableStoreException ex) {
            if (Objects.equals(ex.getErrorCode(), "OTSObjectAlreadyExist")) {
                DeleteTableRequest dreq = new DeleteTableRequest(ownerTable);
                ots.deleteTable(dreq);
                ots.createTable(creq);
            } else {
                throw ex;
            }
        }
    }

    private static void collectPetsByOwner(TableMeta meta) {
        SparkConf sparkConf = new SparkConf().setAppName("CreateReadWrite");
        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            Configuration hadoopConf = new Configuration();
            TableStore.setCredential(
                hadoopConf,
                new Credential(accessKeyId, accessKeySecret, null));
            Endpoint ep = new Endpoint(endpoint);
            TableStore.setEndpoint(hadoopConf, ep);
            TableStoreInputFormat.addCriteria(hadoopConf,
                newCriteria(petTable, meta));
            TableStoreOutputFormat.setOutputTable(hadoopConf, ownerTable);
            
            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                hadoopConf, TableStoreInputFormat.class, PrimaryKeyWritable.class,
                RowWritable.class);
            rdd.mapToPair(
                    (Tuple2<PrimaryKeyWritable, RowWritable> in) -> {
                        PrimaryKey pkey = in._1().getPrimaryKey();
                        String pet = pkey.getPrimaryKeyColumn("name").getValue().asString();
                        Row row = in._2().getRow();
                        String owner = row.getColumn("owner").get(0).getValue().asString();
                        String species = row.getColumn("species").get(0).getValue().asString();
                        return new Tuple2<String, Tuple2<String, String>>(owner,
                            new Tuple2<String, String>(pet, species));
                    })
                .groupByKey()
                .mapToPair(
                    (Tuple2<String, Iterable<Tuple2<String, String>>> in) -> {
                        String owner = in._1();
                        Iterable<Tuple2<String, String>> pets = in._2();

                        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
                        pkCols.add(new PrimaryKeyColumn("owner",
                                PrimaryKeyValue.fromString(owner)));
                        RowPutChange put = new RowPutChange(ownerTable,
                            new PrimaryKey(pkCols));
                        for(Tuple2<String, String> petSpe: pets) {
                            String pet = petSpe._1();
                            String species = petSpe._2();
                            put.addColumn(pet, ColumnValue.fromString(species));
                        }
                        BatchWriteWritable res = new BatchWriteWritable();
                        res.addRowChange(put);
                        return new Tuple2<SeriableText, BatchWriteWritable>(
                            new SeriableText(owner), res);
                    })
                .saveAsNewAPIHadoopFile(
                    "/whatever/path",
                    SeriableText.class,
                    BatchWriteWritable.class,
                    TableStoreOutputFormat.class,
                    hadoopConf);
        }
    }

    public static void main(String[] args) {
        SyncClientInterface ots = getOTSClient();
        try {
            TableMeta petMeta = fetchPetTableMeta(ots);
            createOwnerTable(ots);
            collectPetsByOwner(petMeta);
        } finally {
            ots.shutdown();
        }
    }
}
