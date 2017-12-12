# Change Log

## [Unreleased]

- Move Clojure dependency in the dev profile.
- Emitted event now have "-discovery" added to their `:service` value
- The plugins now always emits all added/removed hosts and services. The discovery stream will only index "added" events if no event exists for this host/service in the index.

## Release 0.2.0

### Breaking changes

- Major naming refactoring.
- The `:pred-fn` global option is replaced by the `:tags` option.

## Release 0.1.

- Support for file based discovery.
- Support for HTTP based discovery.
