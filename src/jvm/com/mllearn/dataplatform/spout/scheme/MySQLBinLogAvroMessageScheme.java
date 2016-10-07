package com.mllearn.dataplatform.spout.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import mypipe.avro.DeleteMutation;
import mypipe.avro.InsertMutation;
import mypipe.avro.UpdateMutation;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * MySQL Binlog序列化信息
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-06 17:46
 */
public class MySQLBinLogAvroMessageScheme implements Scheme {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinLogAvroMessageScheme.class);

    @Override
    public List<Object> deserialize(byte[] ser) {
        SpecificRecord mutationRecord = null;

        try {
            byte mutationType = ser[1];
            byte[] srcBytes = Arrays.copyOfRange(ser, 4, ser.length);
            Decoder decoder = DecoderFactory.get().binaryDecoder(srcBytes, null);

            switch (mutationType) {
            case 0x1:
                DatumReader<InsertMutation> insertReader = new SpecificDatumReader<>(InsertMutation.class);
                mutationRecord = insertReader.read(null, decoder);
                break;
            case 0x2:
                DatumReader<UpdateMutation> updateReader = new SpecificDatumReader<>(UpdateMutation.class);
                mutationRecord = updateReader.read(null, decoder);
                break;
            case 0x3:
                DatumReader<DeleteMutation> deleteReader = new SpecificDatumReader<DeleteMutation>(DeleteMutation.class);
                mutationRecord = deleteReader.read(null, decoder);
                break;
            default:
                LOGGER.error("Deserialize mutation type({}) error.", mutationType);
            }

            LOGGER.debug("Deserialize mutation message is {}.", mutationRecord);
        } catch (Exception ex) {
            LOGGER.error("Deserialize mutation message error.", ex);
        }

        return new Values(mutationRecord);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
