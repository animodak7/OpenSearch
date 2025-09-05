/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.lucene.search.Collector;
import org.apache.lucene.store.AlreadyClosedException;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DataFusionSearch {

    private final ShardViewReferenceManager shardViewReferenceManager;

    public DataFusionSearch(String dir, String[] files) throws IOException {

        shardViewReferenceManager = new ShardViewReferenceManager(dir, files);
    }


    public SearcherSupplier acquireSessionContextSupplier(long contextId, long globalRunTimeId) {
        try {
            ShardView  shardView = shardViewReferenceManager.acquireShardView(this.shardPath);
            SearcherSupplier reader = new SearcherSupplier() {

                @Override
                protected Searcher acquireSearcherInternal() {
                    return new Searcher(
                        contextId,
                        shardView,
                        globalRunTimeId,
                        () -> { }
                    );
                }

                @Override
                protected void doClose() {
                    try {
                        shardViewReferenceManager.release(shardView);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to close", e);
                    } catch (AlreadyClosedException e) {
                        throw new AssertionError(e);
                    }
                }
            };

            return reader;
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            // Should we close the engine?
        }

        return null;
    }
}
