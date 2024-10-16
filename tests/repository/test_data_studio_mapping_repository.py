import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from repository import DataStudioMappingRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton


class TestDataStudioMappingRepository(unittest.TestCase):


    OWNER_ID_INDEX = "owner_id-index"


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_dynamodb_table = Mock()

        Singleton.clear_instance(DataStudioMappingRepository)
        with patch('repository.data_studio_mapping_repository.DataStudioMappingRepository._DataStudioMappingRepository__configure_dynamodb') as mock_configure_resource:

            self.mock_configure_resource = mock_configure_resource
            self.mock_configure_resource.return_value = self.mock_dynamodb_table
            self.data_studio_mapping_repository = DataStudioMappingRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.data_studio_mapping_repository = None
        self.mock_configure_resource = None


    def test_get_active_mappings_success(self):
        """
        Test case for successfully retrieving active data studio mappings for a given owner.

        Expected Result: The method returns a list of active mappings associated with the owner.
        """
        owner_id = 'test_owner_id'

        mock_items = [
            {"owner_id": owner_id, "mapping_id": "map1", "active": True},
            {"owner_id": owner_id, "mapping_id": "map2", "active": True}
        ]
        self.mock_dynamodb_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.OWNER_ID_INDEX,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['mapping_id'], 'map1')
        self.assertEqual(result[1]['mapping_id'], 'map2')


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
        self.mock_dynamodb_table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mappings')

        self.mock_dynamodb_table.query.assert_called_once_with(
            IndexName=self.OWNER_ID_INDEX,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )