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

package org.apache.lucene.codecs.lucene90;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.VectorsWriter;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * An example implementation.
 *
 * NOTE: this example is just meant to help in understanding the API, and is
 * not a proposal for a real implementation.
 */
public class ExampleVectorsWriter extends VectorsWriter {
  private final DocValuesConsumer docValuesWriter;

  public ExampleVectorsWriter(SegmentWriteState state) throws IOException {
    docValuesWriter = new Lucene80DocValuesFormat().fieldsConsumer(state);
  }

  @Override
  public void write(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    docValuesWriter.addBinaryField(field, valuesProducer);
  }

  @Override
  public void merge(MergeState mergeState) throws IOException {
    // In a real implementation, we'd implement merging.
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    docValuesWriter.close();
  }
}
