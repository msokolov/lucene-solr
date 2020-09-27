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


import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.VectorValues.ScoreFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Test Indexing/IndexWriter with vectors */
public class TestVectorValues extends LuceneTestCase {

  private IndexWriterConfig createIndexWriterConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("Lucene90"));
    return iwc;
  }

  // Suddenly add vectors to an existing field:
  public void testUpgradeFieldToVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(newStringField("dim", "foo", Store.NO));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
    }
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeTwoDocs() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[3], ScoreFunction.DOT_PRODUCT));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc2));
      assertEquals("cannot change vector dimension from 4 to 3 for field=\"dim\"", expected.getMessage());
    }
  }

  public void testIllegalScoreFunctionChange() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[4], ScoreFunction.EUCLIDEAN));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc2));
      assertEquals("cannot change vector score function from DOT_PRODUCT to EUCLIDEAN for field=\"dim\"", expected.getMessage());
    }
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("dim", new float[1], ScoreFunction.DOT_PRODUCT));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addDocument(doc2));
        assertEquals("cannot change vector dimension from 4 to 1 for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalScoreFunctionChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("dim", new float[4], ScoreFunction.EUCLIDEAN));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addDocument(doc2));
        assertEquals("cannot change vector score function from DOT_PRODUCT to EUCLIDEAN for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addIndexes(new Directory[]{dir}));
        assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addIndexes(dir));
        assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)}));
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)}));
          assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalMultipleValues() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
      doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc));
      assertEquals("VectorValuesField \"dim\" appears more than once in this document (only one value is allowed per field)",
          expected.getMessage());
    }
  }

  public void testIllegalDimensionTooLarge() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      expectThrows(IllegalArgumentException.class,
          () -> doc.add(new VectorField("dim", new float[VectorValues.MAX_DIMENSIONS + 1], ScoreFunction.DOT_PRODUCT)));

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[1], ScoreFunction.EUCLIDEAN));
      w.addDocument(doc2);
    }
  }

  public void testIllegalEmptyVector() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      Exception e = expectThrows(IllegalArgumentException.class,
          () -> doc.add(new VectorField("dim", new float[0], ScoreFunction.NONE)));
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[1], ScoreFunction.NONE));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  // Write vectors, one segment with with SimpleText, another with default codec, then forceMerge
  public void testDifferentCodecs2() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("SimpleText"));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidVectorFieldUsage() {
    VectorField field = new VectorField("field", new float[2], ScoreFunction.NONE);

    expectThrows(IllegalArgumentException.class, () -> field.setIntValue(14));

    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[1]));

    assertNull(field.numericValue());
  }

  public void testDeleteAllVectorDocs() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Store.NO));
      doc.add(new VectorField("v", new float[]{2, 3, 5}, ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();

      try (DirectoryReader r = w.getReader()) {
        assertNotNull(r.leaves().get(0).reader().getVectorValues("v"));
      }
      w.deleteDocuments(new Term("id", "0"));
      w.forceMerge(1);
      try (DirectoryReader r = w.getReader()) {
        assertNull(r.leaves().get(0).reader().getVectorValues("v"));
      }
    }
  }

  public void testVectorFieldMissingFromOneSegment() throws Exception {
    try (Directory dir = FSDirectory.open(createTempDir());
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Store.NO));
      doc.add(new VectorField("v0", new float[]{2, 3, 5}, ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new VectorField("v1", new float[]{2, 3, 5}, ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.forceMerge(1);
    }
  }

  public void testSparseVectors() throws Exception {
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    int[] fieldDocCounts = new int[numFields];
    float[] fieldTotals= new float[numFields];
    int[] fieldDims = new int[numFields];
    ScoreFunction[] fieldScoreFunctions = new ScoreFunction[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      fieldScoreFunctions[i] = ScoreFunction.fromId(random().nextInt(ScoreFunction.values().length));
    }
    try (Directory dir = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), dir, createIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            float[] v = randomVector(fieldDims[field]);
            doc.add(new VectorField(fieldName, v, fieldScoreFunctions[field]));
            fieldDocCounts[field]++;
            fieldTotals[field] += v[0];
          }
        }
        w.addDocument(doc);
      }

      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          float checksum = 0;
          String fieldName = "int" + field;
          for (LeafReaderContext ctx : r.leaves()) {
            VectorValues vectors = ctx.reader().getVectorValues(fieldName);
            if (vectors != null) {
              docCount += vectors.size();
              while (vectors.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                checksum += vectors.vectorValue()[0];
              }
            }
          }
          assertEquals(fieldDocCounts[field], docCount);
          assertEquals(fieldTotals[field], checksum, 1e-5);
        }
      }
    }
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat();
    }
    return v;
  }

  /*
  public void testCheckIndexIncludesVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("v1", randomVector(3), ScoreFunction.NONE));
        w.addDocument(doc);

        doc.add(new VectorField("v1", randomVector(3), ScoreFunction.NONE));
        doc.add(new VectorField("v2", randomVector(3), ScoreFunction.NONE));
        w.addDocument(doc);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, output);
      assertEquals(1, status.segmentInfos.size());
      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
      // total 3 vector values were indexed:
      assertEquals(3, segStatus.vectorStatus.totalValuevectors);
      // ... across 2 fields:
      assertEquals(2, segStatus.vectorStatus.totalValueFields);

      // Make sure CheckIndex in fact declares that it is testing vectors!
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors..."));
    }
  }
  */

}
