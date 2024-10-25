import unittest
from unittest.mock import MagicMock, patch
from dacite import from_dict

from enums.data_studio import DataStudioMappingStatus
from tests.test_utils import TestUtils
from exception import ServiceException
from model import DataStudioMapping
from service import DataStudioMappingService
from enums import ServiceStatus, DataStudioMappingStatus


class TestDataStudioMappingService(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_studio/'
    TEST_OWNER_ID = 'test_owner_id'
    TEST_USER_ID = 'test_user_id'
    TEST_MAPPING_ID = 'test_mapping_id'


    def setUp(self) -> None:
        self.mock_data_studio_mapping_repository = MagicMock()
        self.data_studio_mapping_service = DataStudioMappingService(self.mock_data_studio_mapping_repository)


    def tearDown(self) -> None:
        self.mock_data_studio_mapping_repository = None
        self.data_studio_mapping_service = None


    def test_get_active_mappings_success(self):
        """
        Test case for successfully retrieving active mappings.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mappings_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_mappings = [
            from_dict(DataStudioMapping, mock_items[0]),
            from_dict(DataStudioMapping, mock_items[1])
        ]
        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock(return_value=mock_mappings)

        result = self.data_studio_mapping_service.get_active_mappings(self.TEST_OWNER_ID)
        self.assertEqual(result, mock_mappings)


    def test_get_active_mappings_empty(self):
        """
        Test case for handling empty data from the repository.
        Expected Result: The service should return an empty list.
        """

        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock(return_value=[])

        result = self.data_studio_mapping_service.get_active_mappings(self.TEST_OWNER_ID)

        self.assertEqual(result, [])


    def test_get_active_mappings_failure(self):
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised by the service layer.
        """

        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock()
        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to retrieve data studio mappings'
        )

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_service.get_active_mappings(self.TEST_OWNER_ID)

        self.assertEqual(context.exception.message, 'Failed to retrieve data studio mappings')
        self.assertEqual(context.exception.status_code, 500)


    def test_get_mapping_success(self):
        """
        Test case for successfully retrieving mapping.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mapping_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_mappings = [
            from_dict(DataStudioMapping, mock_items[0]),
            from_dict(DataStudioMapping, mock_items[1]),
            from_dict(DataStudioMapping, mock_items[2])
        ]
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping = MagicMock(return_value=mock_mappings)

        result = self.data_studio_mapping_service.get_mapping(self.TEST_OWNER_ID, self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(result.draft, mock_mappings[2])
        self.assertEqual(result.draft.revision, self.TEST_OWNER_ID)
        self.assertEqual(result.draft.status, DataStudioMappingStatus.DRAFT.value)
        self.assertEqual(len(result.revisions), 2)


    def test_get_mapping_without_owner_draft(self):
        """
        Test case for successfully retrieving mapping without owners draft.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mapping_without_draft_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_mappings = [
            from_dict(DataStudioMapping, mock_items[0]),
            from_dict(DataStudioMapping, mock_items[1]),
        ]
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping = MagicMock(return_value=mock_mappings)

        result = self.data_studio_mapping_service.get_mapping(self.TEST_OWNER_ID, self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(result.draft, None)
        self.assertEqual(len(result.revisions), 2)


    def test_get_mapping_without_draft_and_revisions(self):
        """
        Test case for successfully retrieving mapping without draft & revisions.
        """
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping = MagicMock(return_value=[])

        result = self.data_studio_mapping_service.get_mapping(self.TEST_OWNER_ID, self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(result.draft, None)
        self.assertEqual(len(result.revisions), 0)


    def test_get_mapping_with_multiple_users_draft_and_revisions_must_return_active_users_draft_and_all_revisions(self):
        """
        Test case for successfully retrieving active users draft and all revision mappings.
        Description:
            Input file contains 3 entries with one PUBLISHED & two DRAFT where one of the DRAFT is TEST_OWNER_ID.
            Assert is checking whether draft revision is equals to TEST_OWNER_ID & revision length is only one.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mapping_with_multiple_users_draft_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        mock_mappings = [
            from_dict(DataStudioMapping, mock_items[0]),
            from_dict(DataStudioMapping, mock_items[1]),
            from_dict(DataStudioMapping, mock_items[2])
        ]
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping = MagicMock(return_value=mock_mappings)

        result = self.data_studio_mapping_service.get_mapping(self.TEST_OWNER_ID, self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(result.draft.revision, mock_mappings[2].revision)
        self.assertEqual(len(result.revisions), 1)
        
    
    def test_get_mapping_failure(self):
        """
        Test case for handling failure in the repository layer.
        """
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping = MagicMock()
        self.data_studio_mapping_service.data_studio_mapping_repository.get_mapping.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to retrieve data studio mapping'
        )

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_service.get_mapping(self.TEST_OWNER_ID, self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(context.exception.message, 'Failed to retrieve data studio mapping')
        self.assertEqual(context.exception.status_code, 500)


    @patch('nanoid.generate')
    def test_create_mapping_success(self, mock_nanoid):
        """
        Test case for successfully creating a new data studio mapping that should call the repository and return the mapping object.
        """
        mock_nanoid.return_value = 'mocked_mapping_id'
        self.data_studio_mapping_service.data_studio_mapping_repository.create_mapping = MagicMock()

        expected_data = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )

        result = self.data_studio_mapping_service.create_mapping(self.TEST_USER_ID, self.TEST_OWNER_ID)

        self.assertIsInstance(result, DataStudioMapping)
        self.assertEqual(result, expected_data)

        self.data_studio_mapping_service.data_studio_mapping_repository.create_mapping.assert_called_once_with(expected_data)


    @patch('nanoid.generate')
    def test_create_mapping_should_raise_exception_when_repository_call_fails(self, mock_nanoid):
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised.
        """
        mock_nanoid.return_value = 'mocked_mapping_id'

        mock_create_mapping = self.data_studio_mapping_service.data_studio_mapping_repository.create_mapping = MagicMock()
        mock_create_mapping.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to create data studio mapping'
        )

        expected_data = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_service.create_mapping(self.TEST_USER_ID, self.TEST_OWNER_ID)

        self.assertEqual(context.exception.message, 'Failed to create data studio mapping')
        self.assertEqual(context.exception.status_code, 500)

        self.data_studio_mapping_service.data_studio_mapping_repository.create_mapping.assert_called_once_with(expected_data)
