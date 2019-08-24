/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.inmemory

import org.almibe.ligature.Dataset
import org.almibe.ligature.Store
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream
import kotlin.concurrent.withLock

class InMemoryStore: Store {
    private var open = true
    private var datasets = mutableMapOf<String, InMemoryDataset>()
    private var lock = ReentrantLock()

    override fun close() {
        lock.withLock {
            open = false
            datasets.clear()
        }
    }

    override fun deleteDataset(name: String) {
        lock.withLock {
            if (open) {
                datasets.remove(name)
            } else {
                throw IllegalStateException("Store is closed.")
            }
        }
    }

    override fun getDataset(name: String): Dataset {
        return lock.withLock {
            if (open) {
                val dataset = datasets.get(name)
                return if (dataset != null) {
                    dataset
                } else {
                    val newDataset = InMemoryDataset(name)
                    datasets.put(name, newDataset)
                    newDataset
                }
            } else {
                throw IllegalStateException("Store is closed.")
            }
        }
    }

    override fun getDatasetNames(): Stream<String> {
        lock.withLock {
            if (open) {
                return datasets.keys.stream()
            } else {
                throw IllegalStateException("Store is closed.")
            }
        }
    }
}
