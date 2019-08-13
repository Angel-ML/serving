## Angel Serving
**Angel Serving** is standalone industrial serving system for machine/deep learning models, it is designed
to flexible and high-performance.

### Architecture

----

![][1]

### Features
- One can access Angel Serving through gRPC and Restful API

- Angel Serving is a general machine learning serving framework which means models from other training platform can server on Angel Serving. 
There is a pluggable mechanism for the third party platform join in, now we support: Angel, PyTorch and PMML format. Through the PMML format, Angel can server Spark and XGBoost models.

- Similar to TensorFlow Serving, we provide fine grain version control: earliest, latest and specified versions.

- Apart from version control, angel serving also provide fine grain service monitoring:
  - QPS: Query per second
  - Success/Total requests
  - Response time distribution
  - Average response Time

### Setup
1. **Compile Environment Requirements**
   - jdk >=1.8
   - maven >= 3.0.5
   - protobuf >= 3.5.1

2. **Source Code Download**

   ```$xslt
   git clone https://github.com/Angel-ML/serving.git
   ```

3. **Compile**

   Run the following command in the root directory of the source code
   ```$xslt
   mvn clean package -Dmaven.test.skip=true
   ```
   After compiling, a distribution package named `serving-0.1.0-SNAPSHOT-bin.zip` will be generated under dist/target in the root directory.

4. **Distribution Package**
   Unpacking the distribution package, four subdirectories will be generated under the root directory:
   - bin: contains Angel Serving start scripts.
   - conf: contains system config files.
   - lib: contains jars for Angel Serving and dependencies.
   - models: contains trained example models.
   - docs: contains user manual and restful api documentation.

### Deployment Guide
1. **Execution Environment Requirements**
   - jdk >= 1.8
   - set JAVA_HOME

2. **Start Server**

   Run the `serving-submit` with args to start Angel Serving, example:
   ```$xslt
   $SERVING_HOME/bin/serving-submit \
      --port 8500 \
      --rest_api_port 8501 \
      --model_base_path $SERVING_HOME/models/angel/lr/lr-model/ \
      --model_name lr \ 
      --model_platform angel \
      --enable_metric_summary true
   ```

### Documentation

* [User Manual](./docs/serving_doc.md)
* [Restful API](./docs/restful-api.md)

[1]: ./docs/img/AngelServing_framework.png