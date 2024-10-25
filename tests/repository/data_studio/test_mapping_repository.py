from dataclasses import asdict
import unittest
from unittest.mock import MagicMock, Mock, patch
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from enums.data_studio import DataStudioMappingStatus
from model.data_studio import DataStudioMapping
from tests.test_utils import TestUtils
from repository import DataStudioMappingRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton


class TestDataStudioMappingRepository(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_studio/'
    TEST_OWNER_ID = 'test_owner_id'
    TEST_USER_ID = 'test_user_id'
    TEST_MAPPING_ID = 'test_mapping_id'


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
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mappings_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
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
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
            FilterExpression=Attr('active').eq(True)
        )
        self.assertEqual(len(result), 0)


    def test_get_active_mappings_failure(self):
        """
        Test case for handling failure while retrieving active data studio mappings due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
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
            self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mappings')

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
            FilterExpression=Attr('active').eq(True)
        )

    
    def test_get_mapping_success(self):
        """
        Test case for successfully retrieving data studio mapping for a given owner & mapping.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mapping_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )
        self.assertEqual(len(result), 3)

    
    def test_get_mapping_return_empty_list_for_non_existing_mapping_id(self):
        """
        Test case for successfully retrieving empty list for non existing mapping id.
        """
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )
        self.assertEqual(len(result), 0)

    
    def test_get_mapping_failure(self):
        """
        Test case for handling failure while retrieving data studio mapping due to a ClientError.
        """
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
            self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mapping')

        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )

    def test_create_mapping_success(self):
        """
        Test case for successfully creating a data studio mapping entry in database.

        Expected Result: The method should successfully call the put item of dynamodb table.
        """
        mapping = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )
        self.mock_table.put_item = MagicMock()

        self.data_studio_mapping_repository.create_mapping(mapping)

        self.mock_table.put_item.assert_called_once_with(Item=asdict(mapping))


    def test_create_mapping_should_raise_exception_when_db_call_fails(self):
        """
        Test case for handling failure while creating a data studio mapping entry in database due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        mapping = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )

        error_response = {
            'Error': {
                'Code': 'ClientError',
                'Message': 'Invalid parameter'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 400
            }
        }

        self.mock_table.put_item.side_effect = ClientError(error_response, 'put_item')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.create_mapping(mapping)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(str(context.exception.message), 'Couldn\'t create the mapping')
