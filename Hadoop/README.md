
## Start Hadoop processes
`start-dfs.sh`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/4ae356ca-74cb-437c-982b-acbcf9458c6e)
`start-yarn.sh`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/7717f2aa-6f51-490a-9888-5a116281c95c)

## verify that it is running with : 
`jps`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/27e099ad-64d3-416f-a411-e367c34e9ae8)

## access the NameNode web interface with 
`http://localhost:50070`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/03e82726-90d0-4f40-a16f-f5a98250866d)

## Create the following tree in the HDFS
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/26416997-c8ea-40eb-baaa-fcf4c27372c9)
`hdfs dfs -mkdir -p BDDC/{JAVA/{TPs,Cours},CPP/{TPs,Cours}}`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/ffa4530d-143d-4bd4-a8f9-775f35911746)
`hdfs dfs -ls -R BDDC`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/9cc8e7a0-71f4-4681-936f-749d60b041d0)

## Create files in a directory
`hdfs dfs -touchz BDDC/CPP/Cours/{CoursCPP1-basic,CoursCPP2-OOP,CoursCPP3-Advenced}`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/14730ab0-6ab0-4670-9da3-187daf2391e9)

## Add content to files 
`echo "CPP course for everyOne!" | hadoop fs -appendToFile - BDDC/CPP/Cours/CoursCPP1-basic`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/74592108-2de1-47e5-943a-cb218590bb80)


## Display Content of files
`hdfs dfs -cat BDDC/CPP/Cours/CoursCPP1-basic`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/fdc52960-076c-48a8-b302-c8fa19babdd7)

## Copy files to another repository
`hdfs dfs -cp -f BDDC/CPP/Cours/{CoursCPP1-basic,CoursCPP2-OOP,CoursCPP3-Advenced} BDDC/JAVA/Cours`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/04b4506e-0459-401a-8022-2709e1e05274)

## Remove a file
`hdfs dfs -rm BDDC/JAVA/Cours/CoursCPP3-Advenced`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/354e09f2-f249-4988-a9a9-6dc085ad6429)

## Rename files
`hdfs dfs -mv BDDC/JAVA/Cours/CoursCPP1-basic BDDC/JAVA/Cours/CoursJAVA2`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/a1602d13-02ba-47a9-835a-e14174e54f35)

## Create files in local
`touch {TP1CPP,TP2CPP,TP1JAVA,TP2JAVA,TP3JAVA}`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/151eb50d-65f4-4d95-b424-36f6f0e57add)


## Copy From Local to HDFS
`hdfs dfs -copyFromLocal {TP1CPP,TP2CPP} SDIA/PYTHON/TPs`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/2adafb1f-5d6b-4d26-aebe-3a5565b8b545)


## Display content of repository 
`hdfs dfs -ls -R SDIA`
![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/a9f4bd2c-6ec9-4863-b886-4e9bb15a4acd)


## Remove file and directory
`hdfs dfs -rm SDIA/PYTHON/TPs/TP1CPP`

`hdfs dfs -rmr BDDC/JAVA`

![image](https://github.com/elmehdi-elkari/Big_Data/assets/70512375/bbd669ea-bd6f-4797-8994-5d05f41483c0)

















