/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import * as lig from "@almibe/ligature"

export const createInMemoryStore = () => new InMemoryStore();

class InMemoryStore implements lig.LigatureStore {
  collection(collectionName: string): Promise<lig.LigatureCollection> {
    throw new Error("Method not implemented.");
  }
  deleteCollection(collectionName: string): Promise<null> {
    throw new Error("Method not implemented.");
  }
  allCollections(): Promise<any> {
    throw new Error("Method not implemented.");
  }
  close(): Promise<null> {
   throw new Error("Method not implemented.");
  }
  details(): Promise<any> {
    throw new Error("Method not implemented.");
  }
}

class InMemoryCollection implements lig.LigatureCollection {
  addStatements(statements: lig.Statements): Promise<null> {
    throw new Error("Method not implemented.");
  }
  removeStatements(statements: lig.Statements): Promise<null> {
    throw new Error("Method not implemented.");
  }
  allStatements(): Promise<any> {
    throw new Error("Method not implemented.");
  }
  newIdentifier(): Promise<string> {
    throw new Error("Method not implemented.");
  }
  matchStatements(pattern: readonly [(string | undefined)?, (string | undefined)?, (string | lig.PlainLiteral | lig.TypedLiteral | undefined)?, (string | undefined)?]): Promise<any> {
    throw new Error("Method not implemented.");
  }
  collectionName(): Promise<string> {
    throw new Error("Method not implemented.");
  }
  addRules(rules: lig.Rules): Promise<null> {
    throw new Error("Method not implemented.");
  }
  removeRules(rules: lig.Rules): Promise<null> {
    throw new Error("Method not implemented.");
  }
  allRules(): Promise<any> {
    throw new Error("Method not implemented.");
  }
  matchRules(pattern: readonly [(string | undefined)?, (string | undefined)?, (string | lig.PlainLiteral | lig.TypedLiteral | undefined)?, (string | undefined)?]): Promise<any> {
    throw new Error("Method not implemented.");
  }
  sparqlQuery(query: any): Promise<any> {
    throw new Error("Method not implemented.");
  }
  wanderQuery(query: any): Promise<any> {
    throw new Error("Method not implemented.");
  }
}
