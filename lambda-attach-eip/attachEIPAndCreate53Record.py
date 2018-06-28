import logging

import boto3

VERSION = '1.0.1'
dryRun = False;  # useful variable to put the script into dry run mode where the function allows it

amiId = 'ami-6d0a5512';
keyPair = 'WebAppKey'
instanceType = 't2.small'
sgId = 'sg-e3cd61a8'
dnsEntry = 'wiki.cloudocentric.com.'
zoneId = 'Z1P246RTIJ4D31'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2Client = boto3.client('ec2')
ec2Resource = boto3.resource('ec2')


def lambda_handler(event, context):
    logger.info('Start attachEIPAndCreate53Record.lambda_handle v{}'.format(VERSION))
    newInstanceId = createInstance(amiId, keyPair, instanceType)
    elasticIP = createAndAssociateToElasticIp(newInstanceId)
    attach(zoneId, dnsEntry, elasticIP)


def createInstance(_amiId, _keyPair, _instanceType):
    # Create the instance
    logger.info('Creating Instance for ami-->{}'.format(amiId))
    instanceDict = ec2Resource.create_instances(
        DryRun=dryRun,
        ImageId=_amiId,
        KeyName=_keyPair,
        InstanceType=_instanceType,
        # SecurityGroupIds=[sgId],
        MinCount=1,
        MaxCount=1
    )
    # Wait for it to launch before assigning the elastic IP address
    instanceDict[0].wait_until_running();
    logger.info('Instance created {}'.format(instanceDict[0].id))

    return instanceDict[0].id;


def createAndAssociateToElasticIp(newInstanceId):
    # Allocate an elastic IP
    eip = ec2Client.allocate_address(DryRun=dryRun, Domain='vpc')
    # Associate the elastic IP address with the instance launched above
    ec2Client.associate_address(
        DryRun=dryRun,
        InstanceId=newInstanceId,
        AllocationId=eip["AllocationId"])

    return eip["PublicIp"]


def attach(_zoneId, _dnsEntry, _elasticIP):
    route53Client = boto3.client('route53')
    # Now add the route 53 record set to the hosted zone for the domain
    route53Client.change_resource_record_sets(
        HostedZoneId=_zoneId,
        ChangeBatch={
            'Comment': 'Add new instance to Route53',
            'Changes': [
                {
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': _dnsEntry,
                        'Type': 'A',
                        'TTL': 60,
                        'ResourceRecords': [
                            {
                                'Value': _elasticIP
                            },
                        ],
                    }
                },
            ]
        })
