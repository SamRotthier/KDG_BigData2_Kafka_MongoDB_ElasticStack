# MongoDB Info and Queries

## Sharding key
De Sharding key is de start tijd.
Dit leek mij een van de interessantste velden om op te zoeken.

## Queries
### Query - One shard
query:
{ starttime: "2022-11-01T11:54:37.000+01:00" }

### Query - All shards
Wanneer we query uitvoeren op de stad
zoals in compas: 
{ "city": "Mortsel" } 
Dan worden alle shards aangesproken

In shell:
Query:
db.rides.find({ "city": "Mortsel" })
db.rides.find({ "city": "Mortsel" }).explain("executionStats")


### Query - With information of user or Vehicle
Query:
{$or: [ { Vehicleid: 1839 }, { name: { $regex: "Simone" }} ]}
