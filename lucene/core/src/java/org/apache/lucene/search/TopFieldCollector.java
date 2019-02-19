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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.util.FutureObjects;

/**
 * A {@link Collector} that sorts by {@link SortField} using
 * {@link FieldComparator}s.
 * <p>
 * See the {@link #create(org.apache.lucene.search.Sort, int, int)} method
 * for instantiating a TopFieldCollector.
 *
 * @lucene.experimental
 */
public abstract class TopFieldCollector extends TopDocsCollector<Entry> {

  // TODO: one optimization we could do is to pre-fill
  // the queue with sentinel value that guaranteed to
  // always compare lower than a real hit; this would
  // save having to check queueFull on each insert

  private static abstract class MultiComparatorLeafCollector implements LeafCollector {

    final LeafFieldComparator comparator;
    final int reverseMul;
    Scorable scorer;

    MultiComparatorLeafCollector(LeafFieldComparator[] comparators, int[] reverseMul) {
      if (comparators.length == 1) {
        this.reverseMul = reverseMul[0];
        this.comparator = comparators[0];
      } else {
        this.reverseMul = 1;
        this.comparator = new MultiLeafFieldComparator(comparators, reverseMul);
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      comparator.setScorer(scorer);
      this.scorer = scorer;
    }
  }

  static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
    return canEarlyTerminateOnDocId(searchSort) ||
           canEarlyTerminateOnPrefix(searchSort, indexSort);
  }

  private static boolean canEarlyTerminateOnDocId(Sort searchSort) {
    final SortField[] fields1 = searchSort.getSort();
    return SortField.FIELD_DOC.equals(fields1[0]);
  }

  private static boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
    if (indexSort != null) {
      final SortField[] fields1 = searchSort.getSort();
      final SortField[] fields2 = indexSort.getSort();
      // early termination is possible if fields1 is a prefix of fields2
      if (fields1.length > fields2.length) {
        return false;
      }
      return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
    } else {
      return false;
    }
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking document scores.
   */
  private static class SimpleFieldCollector extends TopFieldCollector {

    final Sort sort;
    final FieldValueHitQueue<Entry> queue;

    public SimpleFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, int numHits, int totalHitsThreshold, int perSegmentMargin) {
      super(queue, numHits, totalHitsThreshold, perSegmentMargin, sort.needsScores());
      this.sort = sort;
      this.queue = queue;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;

      final LeafFieldComparator[] comparators = queue.getComparators(context);
      final int[] reverseMul = queue.getReverseMul();
      final Sort indexSort = context.reader().getMetaData().getSort();
      final boolean canEarlyTerminate = canEarlyTerminate(sort, indexSort);
      final int leafHitsThreshold = prorateForSegment(Math.max(numHits, totalHitsThreshold), context);

      return new MultiComparatorLeafCollector(comparators, reverseMul) {

        private int leafHits = 0;
        boolean collectedAllCompetitiveHits = false;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          updateMinCompetitiveScore(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
          ++leafHits;
          ++totalHits;
          if (queueFull) {
            if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
              // since docs are visited in doc Id order, if compare is 0, it means
              // this document is larger than anything else in the queue, and
              // therefore not competitive.
              if (canEarlyTerminate) {
                if (totalHits > totalHitsThreshold || leafHits > leafHitsThreshold) {
                  totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
                  throw new CollectionTerminatedException();
                } else {
                  collectedAllCompetitiveHits = true;
                }
              } else if (totalHitsRelation == Relation.EQUAL_TO) {
                // we just reached totalHitsThreshold, we can start setting the min
                // competitive score now
                updateMinCompetitiveScore(scorer);
              }
              return;
            }

            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);
            updateBottom(doc);
            comparator.setBottom(bottom.slot);
            updateMinCompetitiveScore(scorer);
          } else {
            // Startup transient: queue hasn't gathered numHits yet
            final int slot = totalHits - 1;

            // Copy hit into queue
            comparator.copy(slot, doc);
            add(slot, doc);
            if (queueFull) {
              comparator.setBottom(bottom.slot);
              updateMinCompetitiveScore(scorer);
            }
          }
          if (canEarlyTerminate) {
              // When early terminating, stop collecting hits from this leaf once we have its prorated hits.
              if (leafHits > leafHitsThreshold) {
                  totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
                  throw new CollectionTerminatedException();
              }
          }
        }

      };
    }

    /** The total number of documents that matched this query; may be a lower bound in case of early termination. */
    @Override
    public int getTotalHits() {
      return totalHits;
    }

    private int prorateForSegment(int topK, LeafReaderContext leafCtx) {
        // prorate number of hits to collect based on proportion of documents in this leaf (segment).
        // These calculations are based on documents' index sort values being distributed uniformly
        // randomly among segments.
        // p := probability of a top-k document (or any document) being in this segment
        int numDocs = leafCtx.parent.reader().numDocs();
        if (numDocs == 0) {
            return 0;
        }
        double p = (double) leafCtx.reader().numDocs() / numDocs;
        // m := expected number of the topK results in this segment
        double m = p * topK;
        // Add a "safety margin" to ensure the probability of missing a doc is small. This margin
        // corresponds to perSegmentMargin standard deviations of a binomial distribution, which has
        // variance = (p - p^2), where p is the mean of the distribution. With 3 standard deviations
        // above the mean, we expect to return a non top-K document (eg maybe document K+1 instead
        // of K) 0.15% of the time (per segment). With 5 standard deviations, that would happen
        // about 1 in 7 million (different, somehow random) queries.
        double stddev = Math.sqrt(topK * (p - (p * p)));
        return (int) Math.ceil(m + perSegmentMargin * stddev);
    }

  }

  /*
   * Implements a TopFieldCollector when after != null.
   */
  private final static class PagingFieldCollector extends TopFieldCollector {

    final Sort sort;
    int collectedHits;
    final FieldValueHitQueue<Entry> queue;
    final FieldDoc after;
    final int totalHitsThreshold;

    public PagingFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, FieldDoc after, int numHits,
                                int totalHitsThreshold, int perSegmentMargin) {
      super(queue, numHits, totalHitsThreshold, perSegmentMargin, sort.needsScores());
      this.sort = sort;
      this.queue = queue;
      this.after = after;
      this.totalHitsThreshold = totalHitsThreshold;

      FieldComparator<?>[] comparators = queue.comparators;
      // Tell all comparators their top value:
      for(int i=0;i<comparators.length;i++) {
        @SuppressWarnings("unchecked")
        FieldComparator<Object> comparator = (FieldComparator<Object>) comparators[i];
        comparator.setTopValue(after.fields[i]);
      }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - docBase;
      final Sort indexSort = context.reader().getMetaData().getSort();
      final boolean canEarlyTerminate = canEarlyTerminate(sort, indexSort);
      return new MultiComparatorLeafCollector(queue.getComparators(context), queue.getReverseMul()) {

        boolean collectedAllCompetitiveHits = false;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          updateMinCompetitiveScore(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
          totalHits++;

          if (queueFull) {
            // Fastmatch: return if this hit is no better than
            // the worst hit currently in the queue:
            if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
              // since docs are visited in doc Id order, if compare is 0, it means
              // this document is largest than anything else in the queue, and
              // therefore not competitive.
              if (canEarlyTerminate) {
                if (totalHits > totalHitsThreshold) {
                  totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
                  throw new CollectionTerminatedException();
                } else {
                  collectedAllCompetitiveHits = true;
                }
              } else if (totalHitsRelation == Relation.GREATER_THAN_OR_EQUAL_TO) {
                  updateMinCompetitiveScore(scorer);
              }
              return;
            }
          }

          final int topCmp = reverseMul * comparator.compareTop(doc);
          if (topCmp > 0 || (topCmp == 0 && doc <= afterDoc)) {
            // Already collected on a previous page
            return;
          }

          if (queueFull) {
            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);

            updateBottom(doc);

            comparator.setBottom(bottom.slot);
            updateMinCompetitiveScore(scorer);
          } else {
            collectedHits++;

            // Startup transient: queue hasn't gathered numHits yet
            final int slot = collectedHits - 1;
            // Copy hit into queue
            comparator.copy(slot, doc);

            bottom = pq.add(new Entry(slot, docBase + doc));
            queueFull = collectedHits == numHits;
            if (queueFull) {
              comparator.setBottom(bottom.slot);
              updateMinCompetitiveScore(scorer);
            }
          }
        }
      };
    }

  }

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];

  final int numHits;
  final int totalHitsThreshold;
  final int perSegmentMargin;
  final FieldComparator.RelevanceComparator firstComparator;
  final boolean canSetMinScore;
  final int numComparators;
  FieldValueHitQueue.Entry bottom = null;
  boolean queueFull;
  int docBase;
  final boolean needsScores;
  final ScoreMode scoreMode;

  // Declaring the constructor private prevents extending this class by anyone
  // else. Note that the class cannot be final since it's extended by the
  // internal versions. If someone will define a constructor with any other
  // visibility, then anyone will be able to extend the class, which is not what
  // we want.
  private TopFieldCollector(FieldValueHitQueue<Entry> pq, int numHits, int totalHitsThreshold, int perSegmentMargin, boolean needsScores) {
    super(pq);
    this.needsScores = needsScores;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.perSegmentMargin = perSegmentMargin;
    this.numComparators = pq.getComparators().length;
    FieldComparator<?> fieldComparator = pq.getComparators()[0];
    int reverseMul = pq.reverseMul[0];
    if (fieldComparator.getClass().equals(FieldComparator.RelevanceComparator.class)
          && reverseMul == 1 // if the natural sort is preserved (sort by descending relevance)
          && totalHitsThreshold != Integer.MAX_VALUE) {
      firstComparator = (FieldComparator.RelevanceComparator) fieldComparator;
      scoreMode = ScoreMode.TOP_SCORES;
      canSetMinScore = true;
    } else {
      firstComparator = null;
      scoreMode = needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
      canSetMinScore = false;
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return scoreMode;
  }

  protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
    if (canSetMinScore && totalHits > totalHitsThreshold && queueFull) {
      assert bottom != null && firstComparator != null;
      float minScore = firstComparator.value(bottom.slot);
      scorer.setMinCompetitiveScore(minScore);
      totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
    }
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches more than
   *          {@code totalHitsThreshold} hits then its hit count will be a
   *          lower bound. On the other hand if the query matches less than or exactly
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, int totalHitsThreshold) {
    return create(sort, numHits, null, totalHitsThreshold, Integer.MAX_VALUE);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches more than
   *          {@code totalHitsThreshold} hits then its hit count will be a
   *          lower bound. On the other hand if the query matches less than or exactly
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
      int totalHitsThreshold) {
    return create(sort, numHits, after, totalHitsThreshold, Integer.MAX_VALUE);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches
   *          {@code totalHitsThreshold} hits or more then its hit count will be a
   *          lower bound. On the other hand if the query matches less than
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @param perSegmentMargin
   *          the size of the margin (in standard deviations) to add to the totalHitsThreshold when
   *          pro-rated per segment. Pro-rating per segment will collect fewer hits in small
   *          segments, saving time in exchange for a small amount of noise in the ranking. A
   *          typical value is 3. Only use this option for fields whose distribution is expected to
   *          be uniformly random with respect to document update order (e.g.: <i>not</i>
   *          timestamp). To disable prorated early termination, use MAX_INT. This parameter is only
   *          effective when used with totalHitsThreshold &lt;&lt; MAX_INT.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, int totalHitsThreshold, int perSegmentMargin) {
    return create(sort, numHits, null, totalHitsThreshold, perSegmentMargin);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches
   *          {@code totalHitsThreshold} hits or more then its hit count will be a
   *          lower bound. On the other hand if the query matches less than
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @param perSegmentMargin
   *          the size of the margin (in standard deviations) to add to the totalHitsThreshold when
   *          pro-rated per segment. Pro-rating per segment will collect fewer hits in small
   *          segments, saving time in exchange for a small amount of noise in the ranking. A
   *          typical value is 3. Only use this option for fields whose distribution is expected to
   *          be uniformly random with respect to document update order (e.g.: <i>not</i>
   *          timestamp). To disable prorated early termination, use MAX_INT. This parameter is only
   *          effective when used with totalHitsThreshold &lt;&lt; MAX_INT.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
      int totalHitsThreshold, int perSegmentMargin) {
    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException("totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }

    FieldValueHitQueue<Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    if (after == null) {
        return new SimpleFieldCollector(sort, queue, numHits, totalHitsThreshold, perSegmentMargin);
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has " + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      return new PagingFieldCollector(sort, queue, after, numHits, totalHitsThreshold, perSegmentMargin);
    }
  }

  /**
   * Creates a CollectorManager wrapping TopFieldCollectors, with additional arguments that control
   * early termination in conjunction with concurrent collection.
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param totalHitsThreshold
   *          the number of docs to count accurately.
   * @param perSegmentMargin
   *          the size of the margin (in standard deviations) to add to the totalHitsThreshold when
   *          pro-rated per segment. Pro-rating per segment will collect fewer hits in small
   *          segments, saving time in exchange for a small amount of noise in the ranking. A
   *          typical value is 3. Only use this option for fields whose distribution is expected to
   *          be uniformly random with respect to document update order (e.g.: <i>not</i>
   *          timestamp). To disable prorated early termination, use MAX_INT. This parameter is only
   *          effective when used with totalHitsThreshold &lt;&lt; MAX_INT.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static CollectorManager<TopFieldCollector, TopFieldDocs> createManager(Sort sort, int numHits,
      FieldDoc after, int totalHitsThreshold, int perSegmentMargin) {
    return new CollectorManager<TopFieldCollector, TopFieldDocs>() {

      @Override
      public TopFieldCollector newCollector() throws IOException {
        return TopFieldCollector.create(sort, numHits, after, totalHitsThreshold, perSegmentMargin);
      }

      @Override
      public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(sort, 0, numHits, topDocs, true);
      }

    };
  }

  /**
   * Populate {@link ScoreDoc#score scores} of the given {@code topDocs}.
   * @param topDocs   the top docs to populate
   * @param searcher  the index searcher that has been used to compute {@code topDocs}
   * @param query     the query that has been used to compute {@code topDocs}
   * @throws IllegalArgumentException if there is evidence that {@code topDocs}
   *             have been computed against a different searcher or a different query.
   * @lucene.experimental
   */
  public static void populateScores(ScoreDoc[] topDocs, IndexSearcher searcher, Query query) throws IOException {
    // Get the score docs sorted in doc id order
    topDocs = topDocs.clone();
    Arrays.sort(topDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));

    final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
    List<LeafReaderContext> contexts = searcher.getIndexReader().leaves();
    LeafReaderContext currentContext = null;
    Scorer currentScorer = null;
    for (ScoreDoc scoreDoc : topDocs) {
      if (currentContext == null || scoreDoc.doc >= currentContext.docBase + currentContext.reader().maxDoc()) {
        FutureObjects.checkIndex(scoreDoc.doc, searcher.getIndexReader().maxDoc());
        int newContextIndex = ReaderUtil.subIndex(scoreDoc.doc, contexts);
        currentContext = contexts.get(newContextIndex);
        final ScorerSupplier scorerSupplier = weight.scorerSupplier(currentContext);
        if (scorerSupplier == null) {
          throw new IllegalArgumentException("Doc id " + scoreDoc.doc + " doesn't match the query");
        }
        currentScorer = scorerSupplier.get(1); // random-access
      }
      final int leafDoc = scoreDoc.doc - currentContext.docBase;
      assert leafDoc >= 0;
      final int advanced = currentScorer.iterator().advance(leafDoc);
      if (leafDoc != advanced) {
        throw new IllegalArgumentException("Doc id " + scoreDoc.doc + " doesn't match the query");
      }
      scoreDoc.score = currentScorer.score();
    }
  }

  final void add(int slot, int doc) {
    bottom = pq.add(new Entry(slot, docBase + doc));
    queueFull = totalHits == numHits;
  }

  final void updateBottom(int doc) {
    // bottom.score is already set to Float.NaN in add().
    bottom.doc = docBase + doc;
    bottom = pq.updateTop();
  }

  /*
   * Only the following callback methods need to be overridden since
   * topDocs(int, int) calls them to return the results.
   */

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    // avoid casting if unnecessary.
    FieldValueHitQueue<Entry> queue = (FieldValueHitQueue<Entry>) pq;
    for (int i = howMany - 1; i >= 0; i--) {
      results[i] = queue.fillFields(queue.pop());
    }
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
    }

    // If this is a maxScoring tracking collector and there were no results,
    return new TopFieldDocs(new TotalHits(getTotalHits(), totalHitsRelation), results, ((FieldValueHitQueue<Entry>) pq).getFields());
  }

  @Override
  public TopFieldDocs topDocs() {
    return (TopFieldDocs) super.topDocs();
  }

  /** Return whether collection terminated early. */
  public boolean isEarlyTerminated() {
    return totalHitsRelation == Relation.GREATER_THAN_OR_EQUAL_TO;
  }
}
