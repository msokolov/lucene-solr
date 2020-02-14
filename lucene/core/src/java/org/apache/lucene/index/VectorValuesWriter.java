/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.VectorsWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * Buffers up pending vector values, then writes them when the segment flushes.
 */
public class VectorValuesWriter {

  private final BinaryDocValuesWriter docValuesWriter;

  public VectorValuesWriter(FieldInfo fieldInfo, Counter bytesUsed) {
    docValuesWriter = new BinaryDocValuesWriter(fieldInfo, bytesUsed);
  }

  public void addValue(int docID, BytesRef binaryValue) {
    docValuesWriter.addValue(docID, binaryValue);
  }

  public void flush(SegmentWriteState state,
                    Sorter.DocMap sortMap,
                    VectorsWriter vectorsWriter) throws IOException {
    DocValuesConsumer writer = new VectorDocValuesConsumer(vectorsWriter);
    docValuesWriter.flush(state, sortMap, writer);
  }

  private static class VectorDocValuesConsumer extends DocValuesConsumer {
    private final VectorsWriter vectorsWriter;

    public VectorDocValuesConsumer(VectorsWriter vectorsWriter) {
      this.vectorsWriter = vectorsWriter;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      vectorsWriter.write(field, valuesProducer);
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      vectorsWriter.close();
    }
  }
}