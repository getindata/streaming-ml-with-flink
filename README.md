# Flink Mleap

Example how to serve ML model in Flink using Mleap

## Getting started

Before serving ML model you need to create one and export it using MLeap. 
Example how to do it https://getindata.com/blog/online-ml-model-serving-using-mleap/

In `./src/main/resources` you can find two random forest models:

- `mleap-example-1`
- `mleap-example-2`

Those models are taking **one double as input** and **result is one double**.

## Bundle loader
We have exported model inside files, so now we need a way to load them to Flink.
To do that, we will use: `com.getindata.bundle.BundleLoader` and its `FileBundleLoader` implementation. 
This allows us to load ML model stored locally (it is possible to prepare loader from any cloud storage).

## Streaming API
Firstly we will use MLeap with Streaming API. 
It is done in `FlinkWithMleap` app. To present it stream of three rand numbers was created and 
it is processed using `MleapProcessFunction` which simply makes prediction. Making MLeap prediction is
done in `MleapProcessFunction.processElement` method. 

## SQL
Using MLeap with SQL API is presented in `FlinkWithMleapSql`. To test it `datagen` table is created with random features.
To make prediction UDF is implemented: `MLeapUDF` and  MLeap is used here in `eval` method.
To present how to parametrize this function, actually two version of this function are registered as: 

- Predict
- Predictv2

To show it is working, simple query is executed: 
`SELECT Predict(feature1) as prediction, Predictv2(feature1) as prediction2 FROM Features`