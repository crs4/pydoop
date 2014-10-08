/*
  Reads an AvroParquet file and returns raw, binary encoded, serialized avro objects.
 */

package it.crs4.pydoop;

import java.util.Map;
import java.io.IOException;
import java.lang.InterruptedException;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.Record;

import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;


import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import parquet.hadoop.api.ReadSupport;


/*
  Reads an AvroParquet file and returns raw, binary encoded, serialized avro objects.
 */

class PydoopAvroRecordMaterializer extends RecordMaterializer<Text> {
    private RecordMaterializer<IndexedRecord> materializer;
    public  PydoopAvroRecordMaterializer(RecordMaterializer<IndexedRecord> materializer) {
        this.materializer = materializer;
    }
    @Override 
    public Text getCurrentRecord() {
        // FIXME reuse all that can be reused....
        IndexedRecord record = materializer.getCurrentRecord();
        Schema schema = record.getSchema();
        DatumWriter<GenericRecord> datumWriter = 
            new GenericDatumWriter<GenericRecord>(schema);
        EncoderFactory fact = EncoderFactory.get();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BinaryEncoder enc = fact.binaryEncoder(stream, null);
        try {
            datumWriter.write((GenericData.Record) record, enc);
            enc.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Text(stream.toByteArray());
    }
    
    @Override
    public GroupConverter getRootConverter() {
        return materializer.getRootConverter();
    }
}

class PydoopAvroReadSupport extends ReadSupport<Text> {
    private AvroReadSupport<IndexedRecord> read_support;
    
    public PydoopAvroReadSupport() {
        read_support = new AvroReadSupport<IndexedRecord>();
    }
    public static void setRequestedProjection(Configuration configuration, 
                                              Schema requestedProjection) {
        AvroReadSupport.setRequestedProjection(configuration, requestedProjection);
    }

    public static void setAvroReadSchema(Configuration configuration, 
                                         Schema avroReadSchema) {
        AvroReadSupport.setAvroReadSchema(configuration, avroReadSchema);
    }

    @Override
    public ReadContext init(Configuration configuration, 
                            Map<String, String> keyValueMetaData, 
                            MessageType fileSchema) {
        return read_support.init(configuration, keyValueMetaData, fileSchema);
    }
    
    @Override
    public RecordMaterializer<Text> prepareForRead(Configuration configuration, 
                                                   Map<String, String> keyValueMetaData, 
                                                   MessageType fileSchema, 
                                                   ReadContext readContext) {
        RecordMaterializer<IndexedRecord> mat = 
            read_support.prepareForRead(configuration, keyValueMetaData,
                                        fileSchema, readContext);
        return new PydoopAvroRecordMaterializer(mat);
    }
}

/**
 * A specialized {@link parquet.avro.AvroParquetInputFormat} that can be used with pydoop.
 */
public class PydoopAvroParquetInputFormat extends ParquetInputFormat<Text> {
    public PydoopAvroParquetInputFormat() {
        super(PydoopAvroReadSupport.class);
    }

    /**
     * Set the subset of columns to read (projection pushdown). Specified as an Avro
     * schema, the requested projection is converted into a Parquet schema for Parquet
     * column projection.
     * <p>
     * This is useful if the full schema is large and you only want to read a few
     * columns, since it saves time by not reading unused columns.
     * <p>
     * If a requested projection is set, then the Avro schema used for reading
     * must be compatible with the projection. For instance, if a column is not included
     * in the projection then it must either not be included or be optional in the read
     * schema. Use {@link #setAvroReadSchema(org.apache.hadoop.mapreduce.Job,
     * org.apache.avro.Schema)} to set a read schema, if needed.
     * @param job
     * @param requestedProjection
     * @see #setAvroReadSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
     * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
     */
    public static void setRequestedProjection(Job job, Schema requestedProjection) {
        AvroReadSupport.setRequestedProjection(ContextUtil.getConfiguration(job),
                                               requestedProjection);
    }

    /**
     * Override the Avro schema to use for reading. If not set, the Avro schema used for
     * writing is used.
     * <p>
     * Differences between the read and write schemas are resolved using
     * <a href="http://avro.apache.org/docs/current/spec.html#Schema+Resolution">Avro's schema resolution rules</a>.
     * @param job
     * @param avroReadSchema
     * @see #setRequestedProjection(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
     * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
     */
    public static void setAvroReadSchema(Job job, Schema avroReadSchema) {
        AvroReadSupport.setAvroReadSchema(ContextUtil.getConfiguration(job), avroReadSchema);
    }
}


