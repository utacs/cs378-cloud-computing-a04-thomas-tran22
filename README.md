# Please add your team members' names here. 

## Team members' names 

1. Student Name: Zhongyan He

   Student UT EID: zh5555

2. Student Name: Jeffrey Chen

   Student UT EID: jyc752

3. Student Name: Thomas Tran

   Student UT EID: tmt2842

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515

# Add your Project REPORT HERE 

Task 1
01    176115
02    135789
03    105687
04    81901
05    66451
06    120912
07    197069
08    237811
09    248315
10    241862
11    248225
12    264058
13    263555
14    276807
15    275955
16    243535
17    272335
18    331133
19    344987
20    323601
21    319349
22    309277
23    276958
24    227474


Task 2

00DC83118CA675B9A2876C35E3398AF5    1.0
04CD21118F47FA3B2359C65AC063CF0B    1.0
0A0C3F3F29F62642A6DD9D9A087BFBBF    1.0
0B555EC534B208DDD8211150204151D8    1.0
0CAEEA5D95C687B4F7A683D162830BE4    1.0

Task 3 
FD2AE1C5F9F5FBE73A6D6D3D33270571    4095.0
A7C9E60EEE31E4ADC387392D37CD06B8    1260.0
D8E90D724DBD98495C1F41D125ED029A    630.0
E9DA1D289A7E321CC179C51C0C526A73    231.3
74071A673307CA7459BCF75FBD024E09    210.0
28EAF0C54680C6998F0F2196F2DA2E21    180.0
E79402C516CEF1A6BB6F526A142597D4    144.54546
47E338B3C082945EFF04DE6D65915ADE    131.25
B9108122AF332F6262E5456C58804A91    120.0
D149231F39B05AE135FA763EDB358064    120.0



Tasks each have their own repository indicated by task1 task2 an task3. All screenshots of Yarn and the 
VM's running are in A4 Screenshots


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
