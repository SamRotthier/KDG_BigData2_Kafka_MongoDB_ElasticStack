Stappen

# Vorige instanties stoppen
taskkill /F /IM mongod.exe
taskkill /F /IM mongos.exe

# Navigeren
cd ../..
cd test

# Folders aanmaken
mkdir mongodb-cluster\config\rs1
mkdir mongodb-cluster\config\rs2
mkdir mongodb-cluster\config\rs3
mkdir mongodb-cluster\shard1\rs1
mkdir mongodb-cluster\shard1\rs2
mkdir mongodb-cluster\shard2\rs1
mkdir mongodb-cluster\shard2\rs2
mkdir mongodb-cluster\shard3\rs1
mkdir mongodb-cluster\shard3\rs2 
mkdir mongodb-cluster\logs

# Config Servers Opzetten
## Config server 1 aanmaken
mongod --configsvr --replSet configReplSet --port 27019 --dbpath C:\test\mongodb-cluster\config\rs1 --logpath C:\test\mongodb-cluster\logs\config1.log --bind_ip localhost

## Config server 2 aanmaken
mongod --configsvr --replSet configReplSet --port 27020 --dbpath C:\test\mongodb-cluster\config\rs2 --logpath C:\test\mongodb-cluster\logs\config2.log --bind_ip localhost

## Config server 3 aanmaken
mongod --configsvr --replSet configReplSet --port 27021 --dbpath C:\test\mongodb-cluster\config\rs3 --logpath C:\test\mongodb-cluster\logs\config3.log --bind_ip localhost

## Config servers koppelen
### Verbinden met config server
mongosh --port 27019

### Op config server volgend JS script runnen om ze te koppelen
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [
    { _id: 0, host: "localhost:27019" },
    { _id: 1, host: "localhost:27020" },
    { _id: 2, host: "localhost:27021" }
  ]
})

### Nakijken op goede verwerking - er moet een primary zijn
rs.status()


# Shards opzetten
## Shard 1
mongod --shardsvr --replSet shard1ReplSet --port 27022 --dbpath C:\test\mongodb-cluster\shard1\rs1 --logpath C:\test\mongodb-cluster\logs\shard1-rs1.log --bind_ip localhost
mongod --shardsvr --replSet shard1ReplSet --port 27023 --dbpath C:\test\mongodb-cluster\shard1\rs2 --logpath C:\test\mongodb-cluster\logs\shard1-rs2.log --bind_ip localhost
																							  
## Shard 2																							  
mongod --shardsvr --replSet shard2ReplSet --port 27024 --dbpath C:\test\mongodb-cluster\shard2\rs1 --logpath C:\test\mongodb-cluster\logs\shard2-rs1.log --bind_ip localhost																					  
mongod --shardsvr --replSet shard2ReplSet --port 27025 --dbpath C:\test\mongodb-cluster\shard2\rs2 --logpath C:\test\mongodb-cluster\logs\shard2-rs2.log --bind_ip localhost
																							 
## Shard 3                                                                                     
mongod --shardsvr --replSet shard3ReplSet --port 27026 --dbpath C:\test\mongodb-cluster\shard3\rs1 --logpath C:\test\mongodb-cluster\logs\shard3-rs1.log --bind_ip localhost																					  
mongod --shardsvr --replSet shard3ReplSet --port 27027 --dbpath C:\test\mongodb-cluster\shard3\rs2 --logpath C:\test\mongodb-cluster\logs\shard3-rs2.log --bind_ip localhost

## Configureren van de shard replica sets
### Verbinden met shard 1
mongosh --port 27022

### Op shard 1 volgend JS script runnen om de replica sets te koppelen
rs.initiate({
  _id: "shard1ReplSet",
  members: [
    { _id: 0, host: "localhost:27022" },
    { _id: 1, host: "localhost:27023" }
  ]
})

### Nakijken op goede verwerking - er moet een primary zijn
rs.status()

### Verbinden met shard 2
mongosh --port 27024

### Op shard 2 volgend JS script runnen om de replica sets te koppelen
rs.initiate({
  _id: "shard2ReplSet",
  members: [
    { _id: 0, host: "localhost:27024" },
    { _id: 1, host: "localhost:27025" }
  ]
})

### Nakijken op goede verwerking - er moet een primary zijn
rs.status()


### Verbinden met shard 3
mongosh --port 27026

### Op shard 3 volgend JS script runnen om de replica sets te koppelen
rs.initiate({
  _id: "shard3ReplSet",
  members: [
    { _id: 0, host: "localhost:27026" },
    { _id: 1, host: "localhost:27027" }
  ]
})

### Nakijken op goede verwerking - er moet een primary zijn
rs.status()

# Mongos Router
## Mongos Router starten
mongos --configdb configReplSet/localhost:27019,localhost:27020,localhost:27021 --port 27017 --logpath C:\test\mongodb-cluster\logs\mongos.log --bind_ip localhost

## Shard toevoegen aan Mongos Router
### Verbinden met router
mongosh --port 27017

### Toeveogen van shards
#### Toevoegen Shard 1
sh.addShard("shard1ReplSet/localhost:27022,localhost:27023")

#### Toevoegen Shard 2
sh.addShard("shard2ReplSet/localhost:27024,localhost:27025")

#### Toevoegen Shard 3
sh.addShard("shard3ReplSet/localhost:27026,localhost:27027")

### Controlleer of alles goed is toegevoegd
sh.status()

### Enable sharding in router
sh.enableSharding("VeloData")

### Adding the shardkey
sh.shardCollection("VeloData.rides", { "startTime": 1, "_id": 1 })
(=> Na examen - Betere optie was: sh.shardCollection("VeloData.rides", { "startTime": 1 }))

# Inladen van data
Het inladen van de data is vrij eenvoudig via MongoDB Compass.
Verbind met localhost:27017
Ga daarna VeloData en daarna rides
Daar kan je bovenaan "ADD DATA" klikken en kiezen voor de, eerder gegenereerde, JSON Files in te laden.