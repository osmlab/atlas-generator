# DeployAtlasGeneratorAWS.py - Atlas Generation on EMR

Programmatically deploy Atlas Generation on AWS cluster 

- `DeployAtlasGeneratorAWS.py` - A python script to be executed on a local server that generates Atlas files. 
- `configuration.json` - AWS, Spark, Region configuration
- `README.md` - This file to document script usage.

## Prerequisites
Configure [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).
Python 3.
Install [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) Package.

## Configuration
[configuration.json](https://github.com/atlas-generator/blob/atlas_generation_aws_script/scripts/DeployAtlasGeneratorAWS/configuration.json) is preconfigured for global Atlas Generation.  

### Application config
All required Atlas-Generator parameters listed under `configuration` and `border` sections. Optional parameters can be added to the `other` section using the same format: parameter name -> parameter value.  

### EMR config
EMR and Spark configurations tuned to generate global Atlas run with 130Xr5d.2xlarge node instances. Changing the default instance type require adjusting spark.config accordingly. Please follow the [best practices](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/).   

### Region config 
There are 6 predefined regions: America, Europe, Asia, Africa, Oceania or Global. Custom regions can be added with the same format.

## Python3

Make sure python3 is installed. Instructions below for Mac. Also make sure that you source your .zshrc or restart your shell after you perform these steps.

```
brew install pyenv
brew install python
pyenv install 3.8.5
pyenv global 3.8.5
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.zshrc
```

### Python libraries

Install python libraries necessary to execute the application as listed in requirements.txt.
- boto3 - the python libraries to control EC2 instances


```
sudo pip install -r requirements.txt
```

## DeployAtlasGeneratorAWS.py script parameters
- `--help` - Show help message and exit
mandatory parameters:
- `--pbf` - S3 path to OSM sharded PBF folder. 
- `--output` - S3 path Atlas output folder. 
mutually exclusive parameters:
- `--country` - Generate single or group of countries by ISO codes
- `--region` - Generate predefined region in [configuration.json](https://github.com/atlas-generator/blob/atlas_generation_aws_script/scripts/DeployAtlasGeneratorAWS/configuration.json).
required parameters but also configurable in [configuration.json](https://github.com/atlas-generator/blob/atlas_generation_aws_script/scripts/DeployAtlasGeneratorAWS/configuration.json):
```
"s3":{
         "bucket":"",
         "logging":"",
         "atlas_jar":"",
         "atlas_utilities":""
      }
```
- `--bucket` - S3 bucket name.
- `--log` - S3 path to store EMR logs
- `--jar` - S3 path to atlas-generator jar.
- `--utils` - S3 path to atlas
optional parameters:
- `--config` - Path to [configuration.json](https://github.com/atlas-generator/blob/atlas_generation_aws_script/scripts/DeployAtlasGeneratorAWS/configuration.json). Default is same directory.

### run DeployAtlasGeneratorAWS.py script examples
-Single country: USA
`DeployAtlasGeneratorAWS.py 
--country=USA 
--bucket=your_bucket_name
--jar=s3://your_bucket_name/Atlas_Generation/jar/atlas-generator-shaded.jar
--log=s3://your_bucket_name/Atlas_Generation/logging
--pbf=s3://your_bucket_name/OSM_PBF_Sharding
--util=s3://your_bucket_name/Atlas_Generation/utils
--output=s3://your_bucket_name/Atlas_Generation/output
--zone=your_emr_zone`

-Two countries: USA, CAN.
`DeployAtlasGeneratorAWS.py 
--country=USA,CAN 
--bucket=your_bucket_name
--jar=s3://your_bucket_name/Atlas_Generation/jar/atlas-generator-shaded.jar
--log=s3://your_bucket_name/Atlas_Generation/logging
--pbf=s3://your_bucket_name/OSM_PBF_Sharding
--util=s3://your_bucket_name/Atlas_Generation/utils
--output=s3://your_bucket_name/Atlas_Generation/output
--zone=your_emr_zone`

-Global Atlas Generation
`DeployAtlasGeneratorAWS.py 
--region=global 
--bucket=your_bucket_name
--jar=s3://your_bucket_name/Atlas_Generation/jar/atlas-generator-shaded.jar
--log=s3://your_bucket_name/Atlas_Generation/logging
--pbf=s3://your_bucket_name/OSM_PBF_Sharding
--util=s3://your_bucket_name/Atlas_Generation/utils
--output=s3://your_bucket_name/Atlas_Generation/output
--zone=your_emr_zone`