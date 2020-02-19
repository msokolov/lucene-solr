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

package org.apache.lucene.document;

import java.nio.ByteBuffer;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

/**
 * Per-document vector value; {@code float} array with indexed knn graph for fast approximate nearest neighbor search.
 */
public final class VectorField extends Field {

  /**
   * The maximum number of dimensions for a vector field.
   */
  public static final int MAX_DIMS = 2048;

  private static FieldType fieldType(int dimensions) {
    if (dimensions == 0) {
      throw new IllegalArgumentException("VectorField does not support 0 dimensions.");
    }
    if (dimensions > MAX_DIMS) {
      throw new IllegalArgumentException("VectorField does not support greater than " + MAX_DIMS + " dimensions.");
    }

    FieldType type = new FieldType();
    type.setVectorDimension(dimensions);
    type.setDocValuesType(DocValuesType.BINARY);
    type.freeze();
    return type;
  }

  /**
   * Creates a new vector field.
   */
  public VectorField(String name, float[] vector) {
    super(name, encode(vector), fieldType(vector.length));
  }

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
}