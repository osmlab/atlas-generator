#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Control the pbf sharding process on a remote EC2 instance
"""
import argparse
import logging
import os
import sys
import time

import boto3.ec2
import paramiko
from botocore.exceptions import ClientError
from paramiko.auth_handler import AuthenticationException
from scp import SCPClient


VERSION = "0.4.0"
ec2 = boto3.client("ec2")


def setup_logging(default_level=logging.INFO):
    """
    Setup logging configuration
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=default_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("CloudPBFShardControl")


def finish(error_message=None, status=0):
    """exit the process

    Method to exit the Python script. It will log the given message and then exit().

    :param error_message: Error message to log upon exiting the process
    :param status: return code to exit the process with
    """
    if error_message:
        logger.error(error_message)
    else:
        logger.critical("Done")
    exit(status)


class CloudPBFShardControl:
    """Main Class to control sharding process on EC2"""

    def __init__(
        self,
        timeoutMinutes=6000,
        key="",
        instanceId="",
        pbfURL="",
        processes=4,
        quadtree=None,
        s3Folder=None,
        terminate=False,
        templateName="pbf-sharding-ec2-template",
    ):
        self.timeoutMinutes = timeoutMinutes
        self.key = key
        self.instanceId = instanceId
        self.pbfURL = pbfURL
        self.quadtree = quadtree
        self.processes = processes
        self.s3Folder = s3Folder
        self.terminate = terminate
        self.templateName = templateName
        self.shardDir = "/home/ubuntu/pbfSharding/"
        self.shardLogName = "pbfShardGenerator.log"
        self.shardLog = os.path.join(self.shardDir, self.shardLogName)
        self.shardGenName = "pbfShardGenerator.py"
        self.shardGen = os.path.join(self.shardDir, self.shardGenName)

        self.client = None
        self.instanceName = "PBFShardGenerator"

    def prep(self):
        """Prep an EC2 instance to be able to shard a pbf

        Used to prep an EC2 instance for sharding or to create a new AMI.
        If the instance used to execute the sharding is created from a template
        that has already been prepped then this is not necessary unless new
        versions of software need to be pushed to the instance prior to sharding.
        """
        logger.info("Prepare EC2 instance for sharding")
        if self.instanceId == "":
            self.create_instance()
            self.get_instance_info()

        if self.ssh_cmd("mkdir -p {}".format(self.shardDir)):
            finish("Unable to create directory", -1)

        # fetch scripts to complete sharding
        localFiles = [
            self.shardGenName,
            "sharding_quadtree.txt",
        ]
        self.put_files(localFiles, self.shardDir)

        if self.ssh_cmd("chmod u+x {}*.py".format(self.shardDir)):
            finish("Unable to set permissions", -1)
        if self.ssh_cmd("{} prep ".format(self.shardGen)):
            finish("unable to prep instance", -1)

    def shard(self):
        """Shard a pbf URL on an EC2 instance.

        If the CloudPBFShardControl includes an instance ID then sharding will
        be performed on that instance. If no instance ID is defined then it will
        create a new instance.

        The process to shard is started in the background on the EC2 instance
        this script can be stopped, disconnected, or restarted without
        interrupting the sharding process.

        Dependencies:
          - self.instanceId - indicates a running instance or "" to create one
          - self.pbfURL - indicates the pbfURL to download the pbf from
          - self.processes - number of parallel osmium processes
        """
        logger.info("Start Sharding Process...")
        if self.instanceId == "":
            self.create_instance()
            self.get_instance_info()

        if not self.is_sharding_script_running():
            if self.quadtree is not None:
                start_cmd = "nohup {} shard --s3_quadtree_path {} -p {} -P {} > {} 2>&1 &".format(
                    self.shardGen,
                    self.quadtree,
                    self.pbfURL,
                    self.processes,
                    self.shardLog,
                )
            else:
                start_cmd = "nohup {} shard -p {} -P {} > {} 2>&1 &".format(
                    self.shardGen, self.pbfURL, self.processes, self.shardLog
                )
            logger.info(
                "Execute sharding scripts on EC2 instance. {}".format(start_cmd)
            )
            if self.ssh_cmd(start_cmd):
                finish("Unable to start script", -1)
        else:
            logger.info("Detected a running sharding process.")

        # wait for script to complete
        if self.wait_for_sharding_to_complete():
            finish(
                "Timeout waiting for script to complete. TODO - instructions to reconnect.",
                -1,
            )

        # download log
        self.get_files(self.shardLog, "./")

        self.sync()

    def sync(self):
        """Sync an existing instance with pbf shards with s3

        Dependencies:
          - self.instanceId - indicates a running instance or "" to create one
          - self.s3Folder - the S3 bucket and folder path to push the shards
          - self.terminate - indicates if the EC2 instance should be terminated
        """
        if self.s3Folder is None:
            logger.warning(
                "No S3 output folder specified, skipping s3 sync. Use -o 's3folder/path' to sync to s3"
            )
            return
        logger.info(
            "Syncing EC2 instance pbf output with S3 bucket {}.".format(self.s3Folder)
        )
        # push sharded pbfs to s3
        if self.ssh_cmd("{} sync --out={}".format(self.shardGen, self.s3Folder)):
            finish("Unable to sync with S3", -1)
        # terminate instance
        if self.terminate:
            self.terminate_instance()

    def clean(self):
        """Clean a running Instance of all produced folders and files

        This readies the instance for a clean shard run or terminates an EC2
        instance completely.

        Dependencies:
          - self.instanceId - indicates a running instance or "" to create one
          - self.terminate - indicates if the EC2 instance should be terminated
        """
        if self.terminate:
            logger.info("Terminating EC2 instance.")
            self.terminate_instance()
        else:
            logger.info("Cleaning up EC2 instance.               ")
            if self.ssh_cmd("{} clean ".format(self.shardGen)):
                finish("unable to clean up instance", -1)

    def create_instance(self):
        """Create Instance from PBFShardGenerator template

        Dependencies:
          - self.templateId
          - self.instanceName
        :return:
        """
        logger.info("Creating PBFShardGenerator EC2 instance from template.")
        try:
            logger.info("Create instance without dry run...")
            response = ec2.run_instances(
                LaunchTemplate={"LaunchTemplateName": self.templateName},
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": self.instanceName}],
                    }
                ],
                MaxCount=1,
                MinCount=1,
                KeyName=self.key,
            )
            self.instanceId = response["Instances"][0]["InstanceId"]
            logger.info("Instance {} was created".format(self.instanceId))
        except ClientError as e:
            finish(e, -1)

    def terminate_instance(self):
        """Terminate Instance

        Dependencies:
          - self.templateId
        """
        logger.info(
            "Terminating PBFShardGenerator EC2 instance {}".format(self.instanceId)
        )
        try:
            response = ec2.terminate_instances(InstanceIds=[self.instanceId])
            logger.info("Instance {} was terminated".format(self.instanceId))
        except ClientError as e:
            finish(e, -1)

    def ssh_connect(self):
        """Connect to an EC2 instance"""
        for timeout in range(16):
            try:
                keyFile = "{}/.ssh/{}.pem".format(os.environ.get("HOME"), self.key)
                key = paramiko.RSAKey.from_private_key_file(keyFile)
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                logger.debug(
                    "Connecting to {} ... ".format(self.instance["PublicDnsName"])
                )
                self.client.connect(
                    self.instance["PublicDnsName"], username="ubuntu", pkey=key
                )
                logger.info(
                    "Connected to {} ... ".format(self.instance["PublicDnsName"])
                )
                self.scp = SCPClient(self.client.get_transport())
                break
            except AuthenticationException as error:
                logger.error(
                    "Authentication failed: did you remember to create an SSH key? {error}"
                )
                raise error
            except paramiko.ssh_exception.NoValidConnectionsError:
                time.sleep(15)
                continue

    def put_files(self, localFiles, remoteDirectory):
        """Put files from local system onto running EC2 instance"""
        if self.scp is None:
            self.ssh_connect()
        try:
            self.scp.put(localFiles, remoteDirectory)
        except IOException as error:
            logger.error("Unable to copy files. {error}")
            raise error
        # logger.info("Files: ", localFiles, " uploaded to: ", remoteDirectory)

    def get_files(self, remoteFiles, localDirectory):
        """Get files from running ec2 instance to local system"""
        if self.scp is None:
            self.ssh_connect()
        try:
            self.scp.get(remoteFiles, localDirectory)
        except IOException as error:
            logger.error("Unable to copy files. {error}")
            raise error
        logger.debug("Files: ", remoteFiles, " downloaded to: ", localDirectory)

    def ssh_cmd(self, cmd, quiet=False):
        """Issue an ssh command on the remote EC2 instance

        :param cmd: the command string to execute on the remote system
        :param quiet: If true, don't display errors on failures
        :returns: Returns the status of the completed ssh command.
        """
        if self.client is None:
            self.ssh_connect()
        logger.debug("Issuing remote command: {} ... ".format(cmd))
        ssh_stdin, ssh_stdout, ssh_stderr = self.client.exec_command(cmd)
        if ssh_stdout.channel.recv_exit_status() and not quiet:
            logger.error(
                " Remote command output:\n\t"
                "\t".join(map(str, ssh_stderr.readlines()))
            )
        return ssh_stdout.channel.recv_exit_status()

    def wait_for_sharding_to_complete(self):
        """Wait for sharding process to complete

        Will block execution while waiting for the completion of the sharding
        script on the EC2 instance. Upon completion of the scrip it will look
        at the log file produced to see if it completed successfully. If the
        sharding script failed then this function will exit.

        :returns: 0 - if sharding process completed successfully
        :returns: 1 - if sharding process timed out
        """
        logger.info("Waiting for sharding script to complete...")
        # wait for up to TIMEOUT seconds for the VM to be up and ready
        for timeout in range(self.timeoutMinutes):
            if not self.is_sharding_script_running():
                logger.info("Sharding script has completed.")
                if self.ssh_cmd(
                    "grep 'CRITICAL Done' {}".format(self.shardLog), quiet=True
                ):
                    logger.error("Sharding script did not complete successfully.")
                    localLog = (
                        "./pbfShardGenerator-" + time.strftime("%Y%m%d%H%M%S") + ".log"
                    )
                    self.get_files(self.shardLog, localLog)
                    logger.error(
                        "---tail of shard log output ({})---\n\t".format(localLog)
                        + "\t".join(map(str, open(localLog, "r").readlines()[-10:]))
                    )
                    finish(status=-1)
                return 0
            time.sleep(60)
        return 1

    def is_sharding_script_running(self):
        """Indicate if sharding process is actively running

        Uses pgrep on the EC2 instance to detect if the sharding process is
        actively running.

        :returns: 0 - if sharding process is NOT running
        :returns: 1 - if sharding process is running
        """
        if self.ssh_cmd("pgrep -P1 -f pbfShardGenerator", quiet=True):
            return 0
        logger.debug("Sharding script is still running ... ")
        return 1

    def start_ec2(self):
        """Start EC2 Instance."""
        logger.info("Starting the PBFShardGenerator EC2 instance.")

        try:
            logger.info("Start instance")
            ec2.start_instances(InstanceIds=[self.instanceId])
            logger.info(response)
        except ClientError as e:
            logger.info(e)

    def stop_ec2(self):
        """Stop EC2 Instance."""
        logger.info("Stopping the PBFShardGenerator EC2 instance.")

        try:
            response = ec2.stop_instances(InstanceIds=[self.instanceId])
            logger.info(response)
        except ClientError as e:
            logger.error(e)

    def get_instance_info(self):
        """Get the info for an EC2 instance.

        Given an EC2 instance ID this function will retrieve the instance info
        for the instance and save it in self.instance.
        """
        logger.info("Getting EC2 Instance {} Info...".format(self.instanceId))
        # wait for up to TIMEOUT seconds for the VM to be up and ready
        for timeout in range(100):
            response = ec2.describe_instances(InstanceIds=[self.instanceId])
            if not response["Reservations"]:
                finish("Instance {} not found".format(self.instanceId), -1)
            if (
                response["Reservations"][0]["Instances"][0].get("PublicIpAddress")
                is None
            ):
                logger.debug(
                    "Waiting for EC2 instance {} to boot...".format(self.instanceId)
                )
                time.sleep(6)
                continue
            self.instance = response["Reservations"][0]["Instances"][0]
            logger.info(
                "EC2 instance: {} booted with name: {}".format(
                    self.instanceId, self.instance["PublicDnsName"]
                )
            )
            break
        for timeout in range(100):
            if self.ssh_cmd("systemctl is-system-running", quiet=True):
                logger.debug(
                    "Waiting for systemd on EC2 instance to complete initialization..."
                )
                time.sleep(6)
                continue
            return
        finish("Timeout while waiting for EC2 instance to be ready", -1)


def parse_args(cloudctl: CloudPBFShardControl) -> argparse.ArgumentParser:
    """Parse user parameters

    :returns: args
    """
    parser = argparse.ArgumentParser(
        description="This script automates the use of EC2 instance to process "
        "pbf files. It is meant to be executed on a laptop with access to the "
        "EC2 controller and an S3 bucket."
    )
    parser.add_argument(
        "-n",
        "--name",
        help="Set EC2 instance name. (Default: {})".format(cloudctl.instanceName),
    )
    parser.add_argument(
        "-t",
        "--template",
        help="Set EC2 template name to create instance from. (Default: {})".format(
            cloudctl.templateName
        ),
    )
    parser.add_argument(
        "-m",
        "--minutes",
        type=int,
        help="Set sharding timeout to number of minutes. (Default: {})".format(
            cloudctl.timeoutMinutes
        ),
    )
    parser.add_argument(
        "-v", "--version", help="Display the current version", action="store_true"
    )
    parser.add_argument(
        "-T",
        "--terminate",
        default=False,
        help="Terminate EC2 instance after successful operation",
        action="store_true",
    )
    subparsers = parser.add_subparsers(
        title="commands",
        description="One of the following commands must be specified when executed. "
        "To see more information about each command and the parameters that "
        "are used for each command then specify the command and "
        'the --help parameter. (e.g. "{} sync --help")'.format(cloudctl.shardGen),
    )
    parser_prep = subparsers.add_parser(
        "prep", help="Prepare the remote system to execute the sharding process."
    )
    parser_prep.add_argument(
        "-k",
        "--key",
        required=True,
        help="KEY - Instance key name to use to login to instance. This key "
        "is expected to be the same name as the key as defined by AWS and the "
        "corresponding pem file must be located in your local '~/.ssh/' "
        "directory and should be a pem file. See the following URL for "
        "instructions on creating a key: "
        "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html. "
        "(e.g. `--key=aws-key`)",
    )
    parser_prep.add_argument(
        "-i", "--id", help="ID - Indicates the ID of an existing VM instance to use"
    )
    parser_prep.set_defaults(func=cloudctl.prep)

    parser_shard = subparsers.add_parser(
        "shard",
        help="Shard a pbf and, if '--out' is set, then push shards to S3 folder",
    )
    parser_shard.add_argument(
        "-i", "--id", help="ID - Indicates the ID of an existing VM instance to use"
    )
    parser_shard.add_argument(
        "-k",
        "--key",
        required=True,
        help="KEY - Instance key name to use to login to instance. This key "
        "is expected to be the same name as the key as defined by AWS and the "
        "corresponding pem file must be located in your local '~/.ssh/' "
        "directory and should be a pem file. See the following URL for "
        "instructions on creating a key: "
        "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html. "
        "(e.g. `--key=aws-key`)",
    )
    parser_shard.add_argument(
        "-o",
        "--out",
        help="Out - The S3 Output directory. (e.g. '--out=atlas-bucket/PBF_Sharding')",
    )
    parser_shard.add_argument(
        "-p",
        "--pbf",
        required=True,
        help="pbf URL - pointer to the pbf to shard. "
        "(e.g. '--pbf=https://download.geofabrik.de/central-america-latest.osm.pbf')",
    )
    parser_shard.add_argument(
        "-P",
        "--processes",
        type=int,
        help="processes - The number of parallel osmium processes to start "
        "(Default: {})".format(cloudctl.processes),
    )
    parser_shard.add_argument(
        "-q",
        "--quadtree",
        help="bucket and path to Quadtree on S3. If not specified then local file is used.",
    )
    parser_shard.set_defaults(func=cloudctl.shard)

    parser_sync = subparsers.add_parser(
        "sync", help="Sync pbf files from instance to S3 folder"
    )
    parser_sync.add_argument(
        "-i",
        "--id",
        required=True,
        help="ID - Indicates the ID of an existing VM instance to use",
    )
    parser_sync.add_argument(
        "-k",
        "--key",
        required=True,
        help="KEY - Instance key name to use to login to instance. This key "
        "is expected to be the same name as the key as defined by AWS and the "
        "corresponding pem file must be located in your local '~/.ssh/' "
        "directory and should be a pem file. See the following URL for "
        "instructions on creating a key: "
        "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html. "
        "(e.g. `--key=aws-key`)",
    )
    parser_sync.add_argument(
        "-o", "--out", required=True, help="Out - The S3 Output directory"
    )
    parser_sync.set_defaults(func=cloudctl.sync)

    parser_clean = subparsers.add_parser("clean", help="Clean up instance")
    parser_clean.add_argument(
        "-i",
        "--id",
        required=True,
        help="ID - Indicates the ID of an existing VM instance to use",
    )
    parser_clean.add_argument(
        "-k",
        "--key",
        required=True,
        help="KEY - Instance key name to use to login to instance. This key "
        "is expected to be the same name as the key as defined by AWS and the "
        "corresponding pem file must be located in your local '~/.ssh/' "
        "directory and should be a pem file. See the following URL for "
        "instructions on creating a key: "
        "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html. "
        "(e.g. `--key=aws-key`)",
    )
    parser_clean.set_defaults(func=cloudctl.clean)

    args = parser.parse_args()
    return args


def evaluate(args, cloudctl):
    """Evaluate the given arguments.

    :param args: The user's input.
    :param cloudctl: An instance of CloudPBFShardControl to use.
    """
    cloudctl.terminate = args.terminate
    if args.version is True:
        logger.critical("This is version {0}.".format(VERSION))
        finish()
    if args.name is not None:
        cloudctl.instanceName = args.name
    if args.template is not None:
        cloudctl.templateName = args.templateName
    if args.minutes is not None:
        cloudctl.timeoutMinutes = args.minutes
    if hasattr(args, "quadtree") and args.quadtree is not None:
        cloudctl.quadtree = args.quadtree
    if hasattr(args, "processes") and args.processes is not None:
        cloudctl.processes = args.processes
    if hasattr(args, "key") and args.key is not None:
        cloudctl.key = args.key
    if hasattr(args, "out") and args.out is not None:
        cloudctl.s3Folder = args.out
    if hasattr(args, "pbf") and args.pbf is not None:
        cloudctl.pbfURL = args.pbf
    if hasattr(args, "id") and args.id is not None:
        cloudctl.instanceId = args.id
        cloudctl.get_instance_info()

    if hasattr(args, "func") and args.func is not None:
        args.func()
    else:
        finish("A command must be specified. Try '-h' for help.")


logger = setup_logging()

if __name__ == "__main__":
    cloudctl = CloudPBFShardControl()
    args = parse_args(cloudctl)
    evaluate(args, cloudctl)
    finish()
