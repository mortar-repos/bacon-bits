#!/bin/bash

cd udfs/java
mvn clean install && cp target/bacon-bits-0.1.0.jar bacon-bits.jar
cd ../..
