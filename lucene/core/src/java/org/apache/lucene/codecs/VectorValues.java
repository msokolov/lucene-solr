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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

/**
 * Holds the vector values for a particular field.
 *
 * @lucene.experimental
 */
public abstract class VectorValues {

  public abstract DocIdSetIterator iterator();

  public abstract int docID();

  public abstract float[] value() throws IOException;

  /**
   * For the given query vector, finds an approximate set of nearest neighbors.
   *
   * @param queryVector the query vector.
   * @param k the number of nearest neighbors to return.
   * @param recallFactor a parameter which controls the recall of the search. Generally, higher values correspond to
   *                     better recall at the expense of more distance computations. The exact meaning of this parameter
   *                     depends on the underlying nearest neighbor implementation.
   */
  public abstract TopDocs findNearestVectors(float[] queryVector, int k, int recallFactor) throws IOException;

  public abstract int dimension();

  public static BytesRef encode(float[] value) {
    ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES * value.length);
    buffer.asFloatBuffer().put(value);
    return new BytesRef(buffer.array());
  }

  public static float[] decode(BytesRef bytes) {
    int numDims = bytes.length / Float.BYTES;
    float[] value = new float[numDims];
    ByteBuffer buffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
    buffer.asFloatBuffer().get(value);
    return value;
  }

  public static double distance(float[] first, float[] second) {
    double sum = 0;
    for (int i = 0; i < first.length; i++) {
      double diff = first[i] - second[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }
}
