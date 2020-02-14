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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.VectorValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestVectorsFormat extends LuceneTestCase {
  private static final String VECTOR_FIELD = "vector";

  public void testBasicIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    int numDoc = 10;
    int dim = 32;
    float[][] values = new float[numDoc][];
    for (int i = 0; i < numDoc; i++) {
      values[i] = randomVector(dim);
      add(writer, i, values[i]);
    }

    writer.commit();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext leafReaderContext : reader.leaves()) {
      VectorValues vectorValues = leafReaderContext.reader().getVectorValues(VECTOR_FIELD);
      assertNotEquals(vectorValues.iterator().nextDoc(), NO_MORE_DOCS);
    }

    reader.close();
    dir.close();
  }

  public void testFindNearestWithWrongDimension() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(Codec.forName("Lucene90")));

    float[] value = randomVector(42);
    add(writer, 0, value);

    writer.commit();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    VectorValues vectorValues = leafReader.getVectorValues(VECTOR_FIELD);

    float[] queryVector = randomVector(3);
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> vectorValues.findNearestVectors(queryVector, 10, 1000));
    assertEquals("The query vector doesn't have the same dimension as the indexed vectors.", e.getMessage());

    reader.close();
    dir.close();
  }

  private float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = random().nextFloat();
    }
    return vector;
  }

  private void add(IndexWriter iw, int id, float[] vector) throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new VectorField(VECTOR_FIELD, vector));
    }
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    iw.addDocument(doc);
  }
}
