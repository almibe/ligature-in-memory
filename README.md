# ligature-in-memory
An in-memory implementation of Ligature.
It does this by making use of immutable Scala collections and Monix atomic primitives.
See https://github.com/almibe/ligature for more information.

## Building
This project requires SBT to be installed.
I recommend using https://sdkman.io/ to manage SBT installs.
Once that is set up use `sbt test` to run tests `sbt publishM2` to install the artifact locally.
