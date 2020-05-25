/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use ligature::*;
use im::OrdSet;
use im::HashMap;
use futures::Stream;

struct InMemoryStore {
    store: HashMap<String, OrdSet<Statement>>
}

impl LigatureStore for InMemoryStore {
    fn read_tx(&self) -> &dyn ReadTx {
        unimplemented!()
    }

    fn write_tx(&self) -> &dyn WriteTx {
        unimplemented!()
    }

    fn close(&self) {
        unimplemented!()
    }

    fn is_open(&self) -> bool {
        unimplemented!()
    }
}

pub fn ligature_in_memory() -> Box<dyn LigatureStore> {
    Box::new(InMemoryStore { store: HashMap::new() })
}

struct InMemoryReadTx {

}

impl ReadTx for InMemoryReadTx {
    fn collections(&self) -> &dyn Stream<Item=CollectionName> {
        unimplemented!()
    }

    fn collections_prefix(&self, prefix: CollectionName) -> &dyn Stream<Item=CollectionName> {
        unimplemented!()
    }

    fn collections_range(&self, from: CollectionName, to: CollectionName) -> &dyn Stream<Item=CollectionName> {
        unimplemented!()
    }

    fn all_statements(&self, collection: CollectionName) -> &dyn Stream<Item=Statement> {
        unimplemented!()
    }

    fn match_statements(&self, collection: CollectionName, subject: Option<Entity>, predicate: Option<Predicate>, object: Option<Object>, context: Option<Entity>) -> &dyn Stream<Item=Statement> {
        unimplemented!()
    }

    fn match_statements_range(&self, collection: CollectionName, subject: Option<Entity>, predicate: Option<Predicate>, range: Option<Range>, context: Option<Entity>) -> &dyn Stream<Item=Statement> {
        unimplemented!()
    }

    fn cancel(&self) {
        unimplemented!()
    }

    fn is_open(&self) -> bool {
        unimplemented!()
    }
}

struct InMemoryWriteTx {

}

impl WriteTx for InMemoryWriteTx {
    fn create_collection(&self, collection: CollectionName) {
        unimplemented!()
    }

    fn delete_collection(&self, collection: CollectionName) {
        unimplemented!()
    }

    fn new_entity(&self, collection: CollectionName) -> Entity {
        unimplemented!()
    }

    fn remove_entity(&self, collection: CollectionName, entity: Entity) {
        unimplemented!()
    }

    fn add_statement(&self, collection: CollectionName, statement: Statement) {
        unimplemented!()
    }

    fn remove_statement(&self, collection: CollectionName, statement: Statement) {
        unimplemented!()
    }

    fn commit(&self) {
        unimplemented!()
    }

    fn cancel(&self) {
        unimplemented!()
    }

    fn is_open(&self) -> bool {
        unimplemented!()
    }
}
