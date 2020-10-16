# cloudPBFShardController - PBF Sharding on EC2 Controller

This group of scripts can be used to export the process of sharding a pbf file. In order to achieve efficient distributed processing of global OSM data, this sharding process shards a planet pbf (or other large pbf file) into many small pbf files (mostly with a size of 2~5 MB). Atlas uses a dynamic quadtree of Slippy tiles to shard global OSM data.

The root of the quadtree is the single slippy tile at zoom level 0 covering the whole world. Starting from the root, the sharding process does recursive quadtree-splitting on the leaf nodes of the tree, until the amount of data in each leaf is lower than a predefined threshold. To shard one node, the Slippy tile corresponding to the node is split into 4 smaller tiles on the next zoom level, and each smaller tile will become a new child of the node. The end result of the process is a quadtree where each node corresponds to a Slippy tile, and the depth of the node corresponds to the zoom level of the tile. The final leaf nodes define the bounding boxes of all the shards.

Since the sharding process can take a great deal of memory and processing it is recommended to execute the sharding in the cloud. This folder contains the following files to facilitate starting a VM, executing the sharding processing, pushing the resulting sharded pbf files to an object store, and terminating the VM.

- `sharding_quadtree.txt` - An example sharding quadtree plan. This can be updated or replaced if needed.
- `README.md` - This file to document the reason and usage of these scripts.
- `pbfShardGenerator.py` - A python script to be executed on the virtual machine in the cloud. This script performs the actual sharding of the pbf file and also pushes the results to an S3 object store.
- `cloudPBFShardControl.py` - A python script to be executed on a local server that controls the creation and termination of cloud VMs. This scrip also executes the `pbfShardGenerator.py` on the remote VM to start the sharding process.

## Prerequisites

### AWS EC2 and S3

To execute the entire sharding process the user will need access to an AWS account with EC2 and S3 resources. Please visit the [AWS website](https://aws.amazon.com/) to create and manage your account. This script makes use of [AWS EC2](https://aws.amazon.com/ec2/) resources to execute the sharding process and [AWS S3](https://aws.amazon.com/s3) object store to save the sharded pbf results. To communicate with the AWS console and control the resources the scripts needs access the [AWS CLI](https://aws.amazon.com/cli/). To execute the AWS CLI you will need an "Access Key" and "Secret Access Key". These you will need to get from your AWS administrator. You will also need to set the default region. (e.g. "us-west-1").

- [Install](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
  and then
  [configure](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
  AWS CLI

### AWS Key Pair

To be able to execute commands on the EC2 instance that is created the scripts need to be able to ssh to the EC2 instance. To allow the script to ssh to the EC2 instance you need to create an AWS key pair.

- [Create an AWS key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair)

Make sure that your "my-key-pair.pem" file that is produced during this process is placed in your ~/.ssh/ directory and that the permissions are set correctly as the instructions indicate. Once a key pair has been created you may also need to adjust your [AWS security Groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html) to allow ssh access from your local server. Please see your administrator for information on whether this is required for your setup.

### AWS EC2 Template

This script takes advantage of the ability for AWS to start EC2 instances from templates that have been created from an image of another EC2 image. This scrip assumes that you have created a template from an image of an EC2 instance that you will use to start new instances. Information of [Launch Templates](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-launch-templates.html?icmpid=docs_ec2_console) is available from AWS, but for the purposes of this script and document we will assume that you have installed the python environment on an EC2 instance, created an image from that and then created a launch template from that image to use in the sharding process.

### Python3

Make sure python3 is installed. Instructions below for Mac. Also make sure that you source your .zshrc or restart your shell after you perform these steps.

```
brew install pyenv
brew install python
pyenv install 3.8.5
pyenv global 3.8.5
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.zshrc
```

### Python libraries

Install python libraries necessary to execute the application.

- boto3 - the python libraries to control EC2 instances
- paramiko - ssh agent so the script can ssh to the EC2 instance.

```
sudo pip install boto3 paramiko
```

## Running the cloudPBFShardControl script

There are three major commands you can use with the cloudPBFShardControl.py script. Each one has its own help. The main help display shows the flags and parameters that work with all the commands. The following parameters apply to all commands and must be used on the command line before the command.

- `-h, --help` - Show help message and exit
- `-n NAME, --name NAME` - If creating an EC2 instance, this NAME will be used to override the default EC2 instance name: 'PBFShardGenerator'. The script doesn't use the EC2 instance name so this can be set to any value that the user would like to use.
- `-t TEMPLATE, --template TEMPLATE` - This parameter sets EC2 template name that will be used to create the EC2 instance from. If used, this parameter will override the default: 'pbf-sharding-ec2-template'. At this time a template MUST be specified to operate properly.
- `-m MINUTES, --minutes MINUTES` - This parameter will set the timeout, in minutes, that the script will use when waiting for the sharding process to complete. The default is 6000 minutes.
- `-v, --version` - Display the current version of the script.
- `-T, --terminate` - This flag indicates that the user would like to terminate the EC2 instance after successful operation. If this flag is not specified then the script will leave any EC2 instance used or created running upon completion of the script. There are a few different scenarios where the script will not terminate even if termination is requested. The script will NOT terminate an EC2 instance if an error is encountered when processing any command. The script will also not terminate an EC2 instance if sharding is performed but the sync command is skipped (see --out parameter).

The following parameters are used by one or more of the commands. These parameters may not apply to all command so see the command help file for which of these parameters are accepted, required, or optional for each command.

- `-k KEY, --key KEY` - This parameter is the AWS key pair name created above. This parameter specifies the name of the key as specified in the AWS console. The similarly named pem file must be located in the user's ~/.ssh/ directory. See the following URL for instructions on creating a key: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html. (e.g. `--key=aws-key`)
- `-i ID, --id ID` - This parameter specifies the ID of an existing VM instance to use. If this parameter is specified then the command that is being executed will NOT create a new EC2 instance and will, instead, attempt to connect to an EC2 instance with the given ID. Please note that this is the ID of the instance and not the name or description of the EC2 instance. The script will indicate the ID of the EC2 instance used in the log whether an EC2 instance is created or a running EC2 instance is used.
- `-o OUT, --out OUT` - The S3 Output directory to push sharded PBF files upon successful completion of shard processing. If this parameter is not specified when sharding then the output of the sharding process will not be pushed to an external object store and the EC2 instance will not be terminated even if -T is used. (e.g. '--out=atlas-bucket/PBF_Sharding')
- `-p PBF, --pbf PBF` - This parameter indicates the pointer to the PBF file to shard. (e.g. '--pbf=https://download.geofabrik.de/central-america-latest.osm.pbf')
- `-P PROCESSES, --processes PROCESSES` - The number of parallel osmium processes to start. Note that when processing a large PBF file each osmium process will use a great deal of memory so small numbers of parallel processes is suggested. (Default: 5)

### prep

The prep command can be used to prepare the remote EC2 system for sharding and/or the creating of an image to create a launch template. This script communicates with a running EC2 instance or creates a new EC2 instance, then installs the necessary environment on the EC2 instance in preparation of running the shard processing. This command can be used to prep a clean EC2 instance or it can be used to update an EC2 instance that was started from a template. It is recommended that an image is created from an EC2 instance after successfully executing the prep command to create or update a launch template.

Required Parameters: `-k` or `--key`

Example: Prepare a running instance using my AWS key located here: ~/.ssh/my-key.pem

```
./cloudpbfShardControl.py prep --key=my-key -i i-0d48466b0e91ef786
```

### shard

The shard command is the whole enchilada. You must give a `pbf` URL, and AWS `key`. If final pbf files are to be pushed to an S3 bucket then the `out` parameter indicates the S3 output directory. This command will create an instance, download the pbf file, copy the scripts from this folder to the instance, execute the sharding, and finally, if `out` is specified, push the resulting pbfs to the s3 folder specified. If the global `terminate` flag is provided and the `out` parameter is specified then the instance that is used or created by this operation will be terminated upon successful completion. If the `out` parameter is NOT specified or if there are any errors during processing the pbf then the instance will NOT be terminated.

Required Parameters: `-k` or `--key`, `-p` or `--pbf`

Example: Shard the Central American pbf from GeoFabric.de using a new EC2 instance and my key located here: ~/.ssh/my-key.pem. Once sharding is complete, push the results to the S3 bucket indicated. And finally, is all goes well, terminate the EC2 instance that was created.

```
./cloudPBFShardControl.py --terminate shard --key=my-key --pbf=https://download.geofabrik.de/central-america-latest.osm.pbf --out=my-s3-bucket/PBF_Sharding/output/
```

Example: Shard the Central American pbf from GeoFabric.de using a running EC2 instance and my key located here: ~/.ssh/my-key.pem. This example will not push results to an S3 bucket when sharding is complete. This example also shows how to specify a number of parallel processes to use.

```
./cloudPBFShardControl.py shard --key=my-key --id=i-0d48466b0e91ef786 --pbf=https://download.geofabrik.de/central-america-latest.osm.pbf --processes=20
```

### sync

The sync command can be used to connect to an instance that is running and sync the resulting pbf files from a previous run to S3. This command is generally used after a successful `shard` completion when the `out` parameter was not provided during the sharding. It may also be used to re-sync pbf files from a running instance to the S3 bucket.

Required Parameters: `-i` or `--id`, `-k` or `--key`, `-o` or `--out`

Example: Push shard results that were produced on a running instance to the S3 bucket indicated.

```
./cloudPBFShardControl.py sync --key=my-key --out=my-s3-bucket/PBF_Sharding/output/ --id=i-0d48466b0e91ef786
```

### clean

Clean can be used to clean up a running instance to prep it for a fresh sharding. It can also be used to terminate a running instance without doing anything else by specifying the global `--terminate` flag.

Required Parameters: `-i` or `--id`, `-k` or `--key`

Example: Clean up a running EC2 instance to prep for a new shard run.

```
./cloudPBFShardControl.py clean --key=my-key --id=i-0d48466b0e91ef786
```

Example: Terminate a running EC2 instance.

```
./cloudPBFShardControl.py --terminate clean --key=my-key --id=i-0d48466b0e91ef786
```
