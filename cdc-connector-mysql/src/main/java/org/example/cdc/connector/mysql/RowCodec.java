package org.example.cdc.connector.mysql;

import org.example.cdc.core.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pluggable codec for the "after image" stored in the changelog table. The
 * default is JDK serialization (zero dependencies); a production deployment can
 * swap in JSON/Avro by implementing this interface.
 */
public interface RowCodec {

    String encode(Record row);

    Record decode(String payload, String table, Object primaryKey);

    static RowCodec jdkSerialization() {
        return new JdkSerializationRowCodec();
    }

    final class JdkSerializationRowCodec implements RowCodec {

        @Override
        public String encode(Record row) {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                    out.writeObject(new LinkedHashMap<>(row.values()));
                }
                return Base64.getEncoder().encodeToString(bos.toByteArray());
            } catch (IOException e) {
                throw new IllegalStateException("failed to encode row", e);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Record decode(String payload, String table, Object primaryKey) {
            try {
                byte[] bytes = Base64.getDecoder().decode(payload);
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                    Map<String, Object> values = (Map<String, Object>) in.readObject();
                    return new Record(table, primaryKey, values);
                }
            } catch (IOException | ClassNotFoundException e) {
                throw new IllegalStateException("failed to decode row", e);
            }
        }
    }
}
