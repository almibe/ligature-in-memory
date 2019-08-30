/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.almibe.ligature.inmemory

import org.almibe.ligature.Dataset
import org.almibe.ligature.Store
import java.util.stream.Stream

class InMemoryStore: Store {
    private var open = true
    private var datasets = mutableMapOf<String, InMemoryDataset>()

    @Synchronized override fun close() {
        open = false
        datasets.clear()
    }

    @Synchronized override fun deleteDataset(name: String) {
        if (open) {
            datasets.remove(name)
        } else {
            throw IllegalStateException("Store is closed.")
        }
    }

    @Synchronized override fun getDataset(name: String): Dataset {
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

    @Synchronized override fun getDatasetNames(): Stream<String> {
        if (open) {
            return datasets.keys.stream()
        } else {
            throw IllegalStateException("Store is closed.")
        }
    }
}
