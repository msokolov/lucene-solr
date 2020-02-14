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


import org.apache.lucene.codecs.VectorValues;

/**
 * Per-document vector value; {@code float} array with indexed knn graph for fast approximate nearest neighbor search.
 */
public final class VectorField extends Field {

  /**
   * The maximum number of dimensions for a vector field.
   */
  public static final int MAX_DIMS = 2048;

  /**
   * Creates a new vector field.
   */
  public VectorField(String name, float[] vector) {
    super(name, VectorValues.encode(vector), fieldType(vector.length));
  }

  private static FieldType fieldType(int dimension) {
    if (dimension == 0) {
      throw new IllegalArgumentException("VectorField does not support 0 dimensions.");
    }
    if (dimension > MAX_DIMS) {
      throw new IllegalArgumentException("VectorField does not support greater than " + MAX_DIMS + " dimensions.");
    }

    FieldType type = new FieldType();
    type.setVectorDimension(dimension);
    type.freeze();
    return type;
  }
}