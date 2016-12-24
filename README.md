# MessageHubSimulation
Simulates Data Ingestion in Message Hub
<h2>Introduction</h2>
This code can can be used to Ingest data in multiple batches flexibly through REST Calls. The  sample batches of CSV files with data in range - 10,100,1000,10000,100000,5000,50000,500000 are used for simulation. Data could be divided into batches and ingested using the specialized algorithm developed in this POC.

The Ingestion process is exposed as REST and could be hooked upon by any REST client through REST URL. The process is flexible enough to be called from any application framework. E.g Java, .NET, Streams etc.

Below type of REST url could be used to trigger the data ingestion process

https://hostname/contextroot/rest/IngestService/SimulateIngestion?load=100&batchsize=20

<b>Where:</b>

<b>load:</b> This attribute specifies the overall load to Ingest. Based on this attribute file is selected and Ingested.<br/>
<b>batchsize:</b> This attribute specifies the chunks of batched to be Ingested.
