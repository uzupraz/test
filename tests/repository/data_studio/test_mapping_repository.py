import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from repository import DataStudioMappingRepository
from exception import ServiceException
from enums import ServiceStatus, DataStudioMappingStatus
from utils import Singleton


class TestDataStudioMappingRepository(unittest.TestCase):


    OWNER_ID_INDEX = "owner_id-index"


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_table = Mock()
        Singleton.clear_instance(DataStudioMappingRepository)
        with patch('repository.data_studio.mapping_repository.DataStudioMappingRepository._DataStudioMappingRepository__configure_dynamodb') as mock_configure_dynamodb:
            self.mock_configure_dynamodb = mock_configure_dynamodb
            mock_configure_dynamodb.return_value = self.mock_table
            self.data_studio_mapping_repository = DataStudioMappingRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.app_config = None
        self.aws_config = None
        self.data_studio_mapping_repository = None
        self.mock_configure_dynamodb = None


    def test_get_active_mappings_success(self):
        """
        Test case for successfully retrieving active data studio mappings for a given owner.

        Expected Result: The method returns a list of active mappings associated with the owner.
        """
        owner_id = 'test_owner_id'
        
        mock_items = [
            {
                "owner_id": owner_id,
                "mapping_id": "map1",
                "revision": 1,
                "status": DataStudioMappingStatus.PUBLISHED,
                "active": True,
                "created_by": "creator1",
                "name": "Mapping 1",
                "description": "Test mapping 1",
                "sources": {"source1": "data1"},
                "output": {"output1": "result1"},
                "mapping": {"map_field": "mapped_data"},
                "published_by": "publisher1",
                "published_at": 1633036800
            }
        ]
        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gis_name,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].mapping_id, 'map1')


    def test_get_active_mappings_failure(self):
        """
        Test case for handling failure while retrieving active data studio mappings due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        owner_id = 'test_owner_id'

        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mappings')

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gis_name,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )