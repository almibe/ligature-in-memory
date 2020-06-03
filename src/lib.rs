/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use ligature::*;
use im::OrdSet;
use im::HashMap;
use futures::Stream;

struct InMemoryStore {
    is_open: bool,
    store: HashMap<String, OrdSet<Statement>>
}

impl LigatureStore for InMemoryStore {
    fn read_tx(&self) -> &dyn ReadTx {
        &InMemoryReadTx { is_open: true, store: self }
    }

    fn write_tx(&self) -> &dyn WriteTx {
        &InMemoryWriteTx { is_open: true }
    }

    fn close(&mut self) {
        self.is_open = false;
        self.store = HashMap::new()
    }

    fn is_open(&self) -> bool {
        self.is_open
    }
}

pub fn ligature_in_memory() -> Box<dyn LigatureStore> {
    Box::new(InMemoryStore { is_open: true, store: HashMap::new() })
}

struct InMemoryReadTx<'a> {
    is_open: bool,
    store: &'a InMemoryStore
}

impl <'a>ReadTx for InMemoryReadTx<'a> {
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
    is_open: bool
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
