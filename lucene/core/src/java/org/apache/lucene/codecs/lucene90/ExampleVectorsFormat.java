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

import org.apache.lucene.codecs.VectorsFormat;
import org.apache.lucene.codecs.VectorsReader;
import org.apache.lucene.codecs.VectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A simple example of a vectors format implementation. It delegates to
 * {@link org.apache.lucene.index.BinaryDocValues} to write and read vectors.
 *
 * NOTE: this example is just meant to help in understanding the API, and is
 * not a proposal for a real implementation.
 */
public class ExampleVectorsFormat extends VectorsFormat {
  @Override
  public VectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new ExampleVectorsWriter(state);
  }

  @Override
  public VectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new ExampleVectorsReader(state);
  }
}
