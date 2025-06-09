start cmd /k mongod --shardsvr --replSet rs1 --port 27011 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard1\rs1 --bind_ip localhost
start cmd /k mongod --shardsvr --replSet rs1 --port 27012 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard1\rs2 --bind_ip localhost 
timeout /T 5 /NOBREAK >nul

start cmd /k mongod --shardsvr --replSet rs2 --port 27013 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard2\rs1 --bind_ip localhost 
start cmd /k mongod --shardsvr --replSet rs2 --port 27014 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard2\rs2 --bind_ip localhost 
timeout /T 5 /NOBREAK >nul

start cmd /k mongod --shardsvr --replSet rs3 --port 27015 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard3\rs1 --bind_ip localhost 
start cmd /k mongod --shardsvr --replSet rs3 --port 27016 --dbpath C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\shard3\rs2 --bind_ip localhost 
timeout /T 5 /NOBREAK >nul


start cmd /k mongod --configsvr --replSet configRepl --port 26000 --dbpath "C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\config" --bind_ip localhost --logpath "C:\Users\samro\Desktop\Big_Data_2\98_ProjectUitwerking\MongoDB_Folder\mongo-cluster\config\config.log"
timeout /T 5 /NOBREAK >nul


start cmd /k mongos --configdb configRepl/localhost:26000 --port 27017 --bind_ip localhost
