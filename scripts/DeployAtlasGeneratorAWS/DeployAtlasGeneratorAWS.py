#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Execute Atlas Generation on EMR
@author: Vladimir Lemberg
@history: 11/05/2020 Created
"""

import argparse
import boto3
import json
import logging
import os
import time
from botocore.exceptions import ClientError
from datetime import datetime

VERSION = "0.1.3"


def setup_logging(default_level=logging.WARNING):
    """
    Setup logging configuration
    :param default_level
    """
    logging.basicConfig(level=default_level)
    return logging.getLogger('DeployAtlasScriptOnAws')


def terminate(error_message=None):
    """
    Method to exit the Python script. It will log the given message and then exit().
    :param error_message:
    """
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


def parse_args() -> argparse.Namespace:
    """
    Parse user parameters
    :return: args
    """
    parser = argparse.ArgumentParser(description='This script creates EMR cluster to generate Atlas')
    try:
        parser.add_argument('--bucket', help="s3 bucket name.")
        parser.add_argument('--config', help="Path to configuration.json file.")
        parser.add_argument('--jar', help="S3 path to Atlas jar file.")
        parser.add_argument('--log', help="S3 path for EMR logs.")
        parser.add_argument('--pbf', help="S3 path to Sharded PBF input folder.", required=True)
        parser.add_argument('--output', help="S3 path to Atlas output folder.", required=True)
        parser.add_argument('--util', help="S3 path to Atlas util files.")
        parser.add_argument('--zone', help="EMR zone e.g. us-west-1")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--country', help="Specify country Alpha-3 ISO codes.")
        group.add_argument('--region', help="Country region e.g. America, Europe.")
    except argparse.ArgumentError as e:
        terminate('{}'.format(e))
    return parser.parse_args()


def get_key_val(json_dict: dict, json_key: str):
    """
    Access dictionary elements
    :param json_dict
    :param json_key
    :return: value for the given key
    """
    try:
        value = json_dict[json_key]
    except KeyError as e:
        terminate('JSON Key {} is unknown.'.format(e))
    if not value or value is None:
        terminate("value for key '{}' is empty in configuration.json".format(json_key))
    return value


def get_json_local(json_file: str) -> dict:
    """
    Read local JSON
    :param: json_file
    :return: json content
    """
    if os.path.exists(json_file):
        with open(json_file, "r") as read_file:
            try:
                json_content = json.load(read_file)
            except Exception as e:
                terminate('JSON({}):{}'.format(json_file, e))
    else:
        terminate('Cant open {}'.format(json_file))
    return json_content


def get_json_s3(s3bucket: str, s3key: str) -> dict:
    """
    Read JSON stored on S3
    :param: s3bucket name
    :param: s3key
    :return: json content
    """
    try:
        s3 = boto3.resource('s3')
        json_obj = s3.Object(s3bucket, s3key)
        json_content = json.loads(json_obj.get()['Body'].read().decode('utf-8'))
    except ClientError as e:
        terminate('{}'.format(e))
    return json_content


def is_key_exist_s3(s3bucket: str, s3key: str) -> bool:
    """
    Check file(key) existence on S3
    :param s3bucket:
    :param s3key:
    :return: true if file exist on S3
    """
    try:
        s3 = boto3.resource('s3')
        s3.Object(s3bucket, s3key).load()
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
    else:
        return True


class DeployAtlasScriptOnAws(object):
    """
    Programmatically deploy global Atlas Generation on AWS cluster
    """

    def __init__(self, arguments, configuration):
        self.args = arguments
        # Atlas application properties
        self.app = get_key_val(configuration, 'application')
        # AWS properties
        self.aws = get_key_val(configuration, 'aws')
        # EMR cluster properties
        self.emr = get_key_val(self.aws, 'emr')
        # S3 bucket properties
        self.s3 = get_key_val(self.aws, 's3')
        # EC2 instance properties
        self.ec2 = get_key_val(self.aws, 'ec2')
        # Spark Config
        self.spark_config = get_key_val(configuration, 'spark')
        # Region Config
        self.region_config = get_key_val(configuration, 'regions')
        # Process required arguments
        self.osm_pbf_folder = self.args.pbf
        self.atlas_destination_folder = self.args.output
        # assign
        self.country_list = None
        self.emr_zone = None
        self.job_flow_id = None
        self.job_name = None
        self.s3bucket = None
        self.s3jar = None
        self.s3log = None
        self.s3util = None

    def run(self):
        # Process arguments
        self.parse_args()
        # Validate application parameters
        self.validate_atlas_param()
        # Generate job name
        self.generate_job_name()
        # Open EMR connection
        conn = boto3.client('emr', region_name=self.emr_zone)
        # Start Spark EMR cluster
        self.start_spark_cluster(conn)
        # Add step 'spark-submit'
        self.step_spark_submit(conn)
        # Describe cluster status until terminated
        self.describe_status_until_terminated(conn)

    def parse_args(self):
        """
        Assign values.
        """
        self.country_list = [[self.args.country]] if self.args.country else self.get_region(self.args.region)
        self.emr_zone = self.args.zone if self.args.zone else get_key_val(self.emr['region'], 'zone')
        self.s3bucket = self.args.bucket if self.args.bucket else get_key_val(self.s3, 'bucket')
        self.s3jar = self.args.jar if self.args.jar else get_key_val(self.s3, 'atlas_jar')
        self.s3log = self.args.log if self.args.log else get_key_val(self.s3, 'logging')
        self.s3util = self.args.util if self.args.util else get_key_val(self.s3, 'atlas_utilities')

    def generate_job_name(self):
        """
        Generate
        """
        self.job_name = "{}.{}".format(self.app['name'],
                                       datetime.now().strftime("%Y%m%d.%H%M"))

    def validate_atlas_param(self):
        """
        Ensure that all Atlas side files exist on S3
        """
        for section in self.app['parameters'].values():
            for param in section.values():
                key = self.s3util.partition(self.s3bucket + '/')[2] + '/' + param
                if not is_key_exist_s3(self.s3bucket, key):
                    terminate("{}/{} doesn't exist".format(self.s3util, param))
        if not is_key_exist_s3(self.s3bucket, self.osm_pbf_folder.partition(
                self.s3bucket + '/')[2] + '/' + 'sharding.txt'):
            terminate("{}/sharding.txt doesn't exist".format(self.s3util))

    def generate_atlas_param(self) -> list:
        """
        Build Atlas-Generator parameter list from configuration file
        :return: parameters list
        """
        parameters = get_key_val(self.app, 'parameters')
        param_list = []
        for section in parameters.values():
            for param, value in section.items():
                param_list.append('-{}={}/{}'.format(param, self.s3util, value))
        return param_list

    def get_region(self, region: str) -> list:
        """
        :param: region
        :return: region iso codes list
        """
        return self.region_config[region.lower()]['iso']

    def start_spark_cluster(self, conn: boto3) -> None:
        """
        :param: EMR client
        :return:
        """
        response = None
        try:
            response = conn.run_job_flow(
                Name=self.job_name,
                LogUri=self.s3log,
                ReleaseLabel=get_key_val(self.aws['emr'], 'version'),
                Applications=get_key_val(self.emr, 'software'),
                Instances={
                    'InstanceGroups': [
                        self.instance_group_template('Driver-1', 'ON_DEMAND', 'MASTER'),
                        self.instance_group_template('Executor-1', 'ON_DEMAND', 'CORE'),
                    ],
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    # optional. default is empty in configuration.json
                    'Ec2SubnetId': self.emr['region']['subnet'],
                },
                Configurations=self.spark_config,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                VisibleToAllUsers=True,
            )
        except ClientError as e:
            self.terminate_cluster(conn)
            terminate(e)

        # Process response to determine if Spark cluster was started
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            terminate("Could not create EMR cluster (status code {})".format(response_code))

        logger.info("Created Spark {} cluster with JobFlowId {}".format(self.aws['emr']['software'], self.job_flow_id))

    def describe_status_until_terminated(self, c: boto3) -> None:
        """
        Describe cluster status
        :param c:
        """
        stop = False
        while stop is False:
            try:
                description = c.describe_cluster(ClusterId=self.job_flow_id)
                state = description['Cluster']['Status']['State']
                if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                    stop = True
            except ClientError as e:
                self.terminate_cluster(c)
                terminate(e)

            logger.info(state)
            time.sleep(30)  # Prevent ThrottlingException

    def terminate_cluster(self, c: boto3) -> None:
        """
        Terminate cluster
        :param c:
        """
        description = c.describe_cluster(ClusterId=self.job_flow_id)
        state = description['Cluster']['Status']['State']
        if state == 'STARTING' or state == 'WAITING':
            try:
                logger.info('Terminating cluster: {}'.format(self.job_flow_id))
                response = c.terminate_job_flows(
                    JobFlowIds=[
                        self.job_flow_id
                    ]
                )
                logger.info('Cluster terminated: {}'.format(self.job_flow_id))
            except ClientError as e:
                terminate(e)

        logger.info(response)
        time.sleep(30)  # Prevent ThrottlingException

    def step_spark_submit(self, c: boto3) -> None:
        try:
            response = c.add_job_flow_steps(
                JobFlowId=self.job_flow_id,
                Steps=self.generate_emr_step(),
            )
        except ClientError as e:
            self.terminate_cluster(c)
            terminate(e)

        logger.info("Added step 'spark-submit' with argument '{}'".format(self.args))
        time.sleep(1)  # Prevent ThrottlingException

    def generate_emr_step(self) -> list:
        """
        Generate EMR job steps based on region list
        :return: list of EMR steps
        """
        steps = []
        try:
            for index, step in enumerate(self.country_list):
                if (len(step)) != 1:
                    terminate("ERROR: atlas_regions.json has wrong format.")
                if index != len(self.country_list) - 1:
                    steps.append(self.emr_step_template("CONTINUE", step[0]))
                else:
                    steps.append(self.emr_step_template("TERMINATE_CLUSTER", step[0]))
        except TypeError as e:
            terminate(e)
        return steps

    def emr_step_template(self, action_on_failure: str, country_list: str) -> dict:
        """
        :param action_on_failure: Continue or Terminate
        :param country_list: country iso codes
        :return: generate EMR step template.
        """
        return {
            'Name': self.app['name'],
            'ActionOnFailure': action_on_failure,
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': self.hadoop_jar_step_args(country_list),
            },
        }

    def hadoop_jar_step_args(self, country_list: str) -> list:
        """
        Generate hadoop step.
        :param country_list:
        :return:
        """
        return ['spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn-cluster',
                '--class', self.app['main_class'],
                self.s3jar,
                '-output={}/output'.format(self.atlas_destination_folder),
                '-countries={}'.format(country_list.upper()),
                '-pbfScheme=zz/xx/yy/zz-xx-yy.pbf',
                '-pbfSharding=dynamic@{}/sharding.txt'.format(self.osm_pbf_folder),
                '-pbfs={}'.format(self.osm_pbf_folder),
                '-sharding=dynamic@{}/sharding.txt'.format(self.osm_pbf_folder),
                ] + self.generate_atlas_param()

    def instance_group_template(self, name: str, market: str, role: str) -> dict:
        """
        Generate EMR instance group template
        :param name:
        :param market:
        :param role:
        :return:
        """
        return {
            'Name': name,
            'Market': market,
            'InstanceRole': role,
            'InstanceType': self.get_instance_type(role),
            'InstanceCount': self.get_instance_count(role),
        }

    def get_instance_type(self, role: str) -> str:
        """
        :param role: MASTER (driver) or CORE (worker)
        :return: EC2 Instance Type
        """
        return self.ec2[role.lower()]['type']

    def get_instance_count(self, role: str) -> int:
        """
        :param role: MASTER (driver) or CORE (worker)
        :return: EC2 instances for EMR cluster
        """
        return self.ec2[role.lower()]['count']


logger = setup_logging()

if __name__ == "__main__":
    # parse arguments
    args = parse_args()
    # load config file
    config = get_json_local(args.config) if args.config \
        else get_json_local('configuration.json')
    # deployment
    DeployAtlasScriptOnAws(args, config).run()
