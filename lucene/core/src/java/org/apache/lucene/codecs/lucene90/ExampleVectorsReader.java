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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.codecs.VectorsReader;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;

/**
 * An example implementation.
 *
 * NOTE: this example is just meant to help in understanding the API, and is
 * not a proposal for a real implementation.
 */
public class ExampleVectorsReader extends VectorsReader {
  private final DocValuesProducer docValuesReader;

  public ExampleVectorsReader(SegmentReadState state) throws IOException {
    this.docValuesReader = new Lucene80DocValuesFormat().fieldsProducer(state);
  }

  @Override
  public void checkIntegrity() throws IOException {
    docValuesReader.checkIntegrity();
  }

  @Override
  public VectorValues getVectorValues(FieldInfo field) throws IOException {
    if (field.getVectorDimension() == 0) {
      return null;
    }

    BinaryDocValues docValues = docValuesReader.getBinary(field);
    return new VectorValues() {
      @Override
      public DocIdSetIterator iterator() {
        return docValues;
      }

      @Override
      public int docID() {
        return docValues.docID();
      }

      @Override
      public float[] value() throws IOException {
        BytesRef vector = docValues.binaryValue();
        return VectorValues.decode(vector);
      }

      @Override
      public int dimension() {
        return field.getVectorDimension();
      }

      @Override
      public TopDocs findNearestVectors(float[] queryVector, int k, int recallFactor) {
        if (queryVector.length != dimension()) {
          throw new IllegalArgumentException(
              "The query vector doesn't have the same dimension as the indexed vectors.");
        }

        // In a real implementation, we'd implement nearest-neighbor search here.
        return new TopDocs(new TotalHits(0L, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
      }
    };
  }

  @Override
  public void close() throws IOException {
    docValuesReader.close();
  }
}
