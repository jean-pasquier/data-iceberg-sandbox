# Iceberg sandbox

## Infra

Rely on docker & docker compose, see [infra](./infra/README.md)

## Playing with the data

Play with the data using PySpark or pyiceberg, see [etl](./etl/README.md)


## Next steps

* Create fine-grained service accounts for each data domain team (customer, product, finance, etc.) to implement the least privilege principle.
* Check [Production deployment Checklist](https://docs.lakekeeper.io/docs/nightly/production/)
* Run the [nimtable rust iceberg compaction service](https://github.com/nimtable/iceberg-compaction)
* Compare with competitors: [unitycatalog](https://github.com/unitycatalog/unitycatalog), [nessie](https://github.com/projectnessie/nessie), etc.
* Check streaming implementations: eg [fluss](https://github.com/apache/fluss)
