import unittest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from tests.test_utils import TestUtils
from repository import DataStudioMappingRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton


class TestDataStudioMappingRepository(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_studio/'


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
        owner_id = 'owner_id'
        
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mappings_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].active, True)
        self.assertEqual(result[1].active, True)


    def test_get_active_mappings_returns_empty_response_when_given_owner_id_does_not_have_active_or_inactive_mappings(self):
        """
        Test case for successfully retrieving empty data when owner does not have any mappings or any active mappings.

        Expected Result: The method returns an empty list mappings.
        """
        owner_id = 'owner_id'
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_active_mappings(owner_id)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )
        self.assertEqual(len(result), 0)


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
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(owner_id),
            FilterExpression=Attr('active').eq(True)
        )