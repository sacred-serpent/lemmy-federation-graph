# Lemmy-Stats-Crawler

Crawls Lemmy instances using nodeinfo and API endpoints, to generate a list of instances and overall details.

## Usage

lemmy-stats-crawler will discover new instances from other instances, but you have to seed it with a set of initial instances using the `--start-instances` argument.

```sh
cargo run -- --start-instances baraza.africa,lemmy.ml
```

For a complete list of arguments, use `--help`

```sh
cargo run -- --help
```

## Neo4j Integration

Information about discovered instances can be stored in a neo4j graph database.
Here is an example command line:

```sh
cargo run -- -m 5 --neo4j-uri 127.0.0.1:7687 --neo4j-user user --neo4j-password password --neo4j-db fediverse --store-jobs-count 16
```
