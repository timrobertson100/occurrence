# GBIF Occurrence

The GBIF Occurrence project is a component of the architecture responsible for search and download of GBIF-mediated occurrence records. For data processing please see the [pipelines](https://github.com/gbif/pipelines) project.

This project handles occurrence web services, downloads, search and maps.

This project has many submodules and each of those has a README which you should read for more detail.

## Building

Jenkins builds this project without a profile, and the produced artifacts (JARs) are used together with the corresponding configuration found in the gbif-configuration project.

This project contains integration tests which use the GBIF `dev` environment and requires configuration of the `appkeys` providing the tokens to interact with the services and to be on a GBIF network.

To skip the integration tests (e.g. working without access to the GBIF dev network) please build using:

e.g. `mvn -Pdev -pl \!occurrence-integration-tests clean install`

## Testing

Run unit and integration tests:

```bash
mvn -Pdev clean verify
```

## Other documentation in this project

* [Occurrence persistence](occurrence-persistence/README.md)
* [Occurrence WS](occurrence-ws/README.md)
* [Occurrence CLI](occurrence-cli/README.md)
* [Occurrence download](occurrence-download/README.md)
* [Occurrence common](occurrence-common/README.md)
* [Occurrence registry sync](occurrence-registry-sync/README.md)
* [Occurrence Hive](occurrence-hive/README.md)
* [Occurrence search](occurrence-search/README.md)
* [Occurrence heatmaps](occurrence-heatmaps/README.md)
* [Occurrence processor](occurrence-processor/README.md)
* [Occurrence processor (Default values)](occurrence-processor/doc/DefaultValues.md)
