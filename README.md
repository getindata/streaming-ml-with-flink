# Flink Mleap

Example how to serve ML model in Flink using Mleap

## Getting started

Before serving ML model you need to create one and export it using MLeap. 
Example how to do it https://getindata.com/blog/online-ml-model-serving-using-mleap/

In `./example//src/main/resources` you can find two random forest models:

- `mleap-example-1`
- `mleap-example-2`

Those models are taking **one double as input** and **result is one double**.

## Bundle loader
We have exported model inside files, so now we need a way to load them to Flink.
To do that, we will use: `com.getindata.mleap.BundleLoader` and its `FileBundleLoader` implementation inside `lib` module.
This allows us to load ML model stored locally (it is possible to prepare loader from any cloud storage).
We provide also `GCSBundleLoader` implementation, you can load your bundle from Cloud Storage.

## Streaming API
Firstly we will use MLeap with Streaming API in package: `com.getindata.datastream`,  
It is done in `FlinkDatastreamWithMleap` app in `example` module. To present it, stream of three random numbers was created, and 
it is processed using `MleapProcessFunction` (from `lib` module), which simply makes prediction. Making MLeap prediction is
done in `MleapMapFunction.map` method. 

## SQL
Using MLeap with SQL API is presented in package `com.getindata.sql` inside `example` module. 
App `FlinkSqlWithMleap` contains e2e example how to use it. To test it `datagen` table is created with random features.
To make prediction UDF is implemented: `MLeapUDF` (in `lib` module) and  MLeap is used here in `eval` method. We make `MleapUDF` very generic. 
Its input arguments and output is casted to proper types based on MLeap model schema. Basically this one UDF allows using different 
models inside your job. To make registrations process of UDFs easier - we implemented MLeapUDFRegistry, which can register UDFs basing
on configuration file.
For test purposes we register two UDFs:

- Predict
- Predictv2

To show it is working, simple query is executed: 
`SELECT Predict(feature1) as prediction, Predictv2(feature1) as prediction2 FROM Features`