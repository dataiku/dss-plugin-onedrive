from dku_plugin_test_utils import dss_scenario

TEST_PROJECT_KEY = "PLUGINTESTONEDRIVE"


def test_run_onedrive_standard_read_write(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="Standard_Read_Write")


def test_run_onedrive_shared_folder_read_write(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="Shared_folder_Read_Write")


def test_run_onedrive_large_folder_listing(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="LARGEFOLDERLISTING")
