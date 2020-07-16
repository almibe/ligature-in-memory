# ligature-keyvalue
An key value store api for Ligature that also contains an in-memory implementation.
It does this by making use of immutable Scala collections.
See https://github.com/almibe/ligature for more information.

| Prefixes | Description           | Scodec Pseudocode                                            | Description                                                                        |
| -------- | --------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| 0        | Collection Name -> ID | Byte ~ String -> Long                                        | Prefix ~ Collection Name -> Collection ID                                          |
| 1        | ID -> Collection Name | Byte ~ Long -> String                                        | Prefix ~ Collection ID -> Collection Name                                          |
| 2        | CollectionNameCounter | Byte -> Long                                                 | Prefix -> Collection ID Count                                                      |
| 3        | SPOC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Prefix ~ Collection ID ~ Type ~ ID ~ Predicate ID ~ Type ~ Object ID ~ Context ID  |
| 4        | SOPC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 5        | PSOC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 6        | POSC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 7        | OSPC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 8        | OPSC                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 9        | CSPO                  | Byte ~ Long ~ Byte ~ Long ~ Long ~ Byte ~ Long ~ Long        | Similar to above                                                                   |
| 10       | CollectionCounter     | Byte ~ Long -> Long                                          | Prefix ~ Collection ID -> Counter Value                                            |
| 11       | NamedEntitiesToID     | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection ID ~ Entity Name -> Entity ID                                  |
| 12       | IDToNamedEntities     | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection ID ~ Entity ID -> Entity Name                                  |
| 13       | AnonymousEntities     | Byte ~ Long ~ Long                                           | Prefix ~ Collection ID ~ Anonymous ID                                              |
| 14       | PredicatesToID        | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection ID ~ Predicate -> Predicate ID                                 |
| 15       | IDToPredicates        | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection ID ~ Predicate ID -> Predicate                                 |
| 16       | LangLiteralToID       | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection ID ~ Lang Literal -> Literal ID                                |
| 17       | IDToLangLiteral       | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection ID ~ Literal ID -> Lang Literal                                |
| 18       | StringToID            | Byte ~ Long ~ String -> Long                                 | Prefix ~ Collection ID ~ String -> Literal ID                                      |
| 19       | IDToString            | Byte ~ Long ~ Long -> String                                 | Prefix ~ Collection ID ~ Literal ID -> String                                      |

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
