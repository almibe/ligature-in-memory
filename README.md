# ligature-keyvalue
A key value store api for Ligature that also contains an in-memory implementation.
It does this by making use of immutable Scala collections.
The main idea of this project is to set up a shared code base for storing Ligature data in key value stores
with maximal code reuse.
This is why the in-memory store uses a single ordered map of ByteVectors to store data instead of using
domain objects directly, since stores like RocksDB or FoundationDB work with bytes and not JVM objects.
See https://github.com/almibe/ligature for more information.

| Prefixes | Name                  | Scodec Pseudocode                                            | Description                                                                        |
| -------- | --------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| 0        | CollectionNameToId    | Byte ~ String -> Long                                        | Prefix ~ Collection Name -> Collection Id                                          |
| 1        | IdToCollectionName    | Byte ~ Long -> String                                        | Prefix ~ Collection Id -> Collection Name                                          |
| 2        | CollectionNameCounter | Byte -> Long                                                 | Prefix -> Collection Id Count                                                      |
| 3        | SPOC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Prefix ~ Collection Id ~ Type ~ Id ~ Predicate Id ~ Type ~ Object Id ~ Context Id  |
| 4        | SOPC                  | Similar to above                                             | Similar to above                                                                   |
| 5        | PSOC                  | Similar to above                                             | Similar to above                                                                   |
| 6        | POSC                  | Similar to above                                             | Similar to above                                                                   |
| 7        | OSPC                  | Similar to above                                             | Similar to above                                                                   |
| 8        | OPSC                  | Similar to above                                             | Similar to above                                                                   |
| 9        | CSPO                  | Similar to above                                             | Similar to above                                                                   |
| 10       | CollectionCounter     | Byte ~ Long -> Long                                          | Prefix ~ Collection Id -> Counter Value                                            |
| 11       | NamedEntitiesToId     | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection Id ~ Entity Name -> Entity Id                                  |
| 12       | IdToNamedEntities     | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection Id ~ Entity Id -> Entity Name                                  |
| 13       | AnonymousEntities     | Byte ~ Long ~ Long                                           | Prefix ~ Collection Id ~ Anonymous Id                                              |
| 14       | PredicatesToId        | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection Id ~ Predicate -> Predicate Id                                 |
| 15       | IdToPredicates        | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection Id ~ Predicate Id -> Predicate                                 |
| 16       | LangLiteralToId       | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection Id ~ Lang Literal -> Literal Id                                |
| 17       | IdToLangLiteral       | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection Id ~ Literal Id -> Lang Literal                                |
| 18       | StringToId            | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection Id ~ String -> Literal Id                                      |
| 19       | IdToString            | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection Id ~ Literal Id -> String                                      |

I probably don't need lookups for boolean, long, or double since I can just use the actual values.

| Type Codes | Value             |
| ---------- | ----------------- |
| 0          | Named Entity      |
| 1          | Anonymous Entity  |
| 2          | Lang Literal      |
| 3          | String            |
| 4          | Boolean           |
| 5          | Long              |
| 6          | Double            |

## Building
This project requires SBT to be installed.
I recommend using https://sdkman.io/ to manage SBT installs.
Once that is set up use `sbt test` to run tests `sbt publishM2` to install the artifact locally.
