from enum import Enum


class ServicePermissions(Enum):
    DATA_TABLE_CREATE_ITEM='DATA_TABLE_CREATE_ITEM'
    DATA_TABLE_DELETE_ITEM='DATA_TABLE_DELETE_ITEM'
    CUSTOM_SCRIPT_SAVE_ITEM='CUSTOM_SCRIPT_SAVE_ITEM'
    CUSTOM_SCRIPT_RELEASE_ITEM='CUSTOM_SCRIPT_RELEASE_ITEM'
    UPDATER_GET_TARGET_LIST='UPDATER_GET_TARGET_LIST'