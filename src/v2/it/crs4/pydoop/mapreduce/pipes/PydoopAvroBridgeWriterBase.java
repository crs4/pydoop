package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;

import static it.crs4.pydoop.mapreduce.pipes.Submitter.AvroIO;


public abstract class PydoopAvroBridgeWriterBase
    extends RecordWriter<Text, Text> {

  protected RecordWriter actualWriter;

  protected List<GenericRecord> getOutRecords(
      List<Text> inRecords, List<Schema> schemas) throws IOException {
    if (inRecords.size() != schemas.size()) {
      throw new RuntimeException("records and schemas must have equal size");
    }
    List<GenericRecord> outRecords = new ArrayList<GenericRecord>();
    Iterator<Text> iterRecords = inRecords.iterator();
    Iterator<Schema> iterSchemas = schemas.iterator();
    while (iterRecords.hasNext() && iterSchemas.hasNext()) {
      DatumReader<GenericRecord> reader =
          new GenericDatumReader<GenericRecord>(iterSchemas.next());
      DecoderFactory fact = DecoderFactory.get();
      Decoder dec = fact.binaryDecoder(iterRecords.next().copyBytes(), null);
      outRecords.add(reader.read(null, dec));
    }
    return outRecords;
  }

  protected void write(List<GenericRecord> outRecords, AvroIO mode)
      throws IOException, InterruptedException {
    switch (mode) {
    case K:
      actualWriter.write(outRecords.get(0), NullWritable.get());
      break;
    case V:
      // Parquet writer does not accept a NullWritable key
      actualWriter.write(null, outRecords.get(0));
      break;
    case KV:
      actualWriter.write(outRecords.get(0), outRecords.get(1));
      break;
    default:
      throw new RuntimeException("Invalid Avro I/O mode");
    }
  }

  public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualWriter.close(context);
  }

}
