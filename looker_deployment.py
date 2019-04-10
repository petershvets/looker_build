import argparse
import json
from collections import OrderedDict
import requests
import datetime
import subprocess
import os
import shutil
import re
import logging
import fileinput
import sys
from collections import defaultdict
import traceback
import copy

class ProcessException(Exception):
    pass

# Logging
#Log levels
_MESSAGE = 0
_INFO = 1
_DEBUG = 2
_EXTRA = 3
_WARNING = -2
_ERROR = -1
_DEBUG_LEVEL = 2

_DEBUG_CONF_LEVEL = {
    "DEBUG": _DEBUG,
    "EXTRA": _EXTRA,
    "NORMAL": _INFO
}

_LOGGER = None

_MESSAGE_TEXT = {
    _MESSAGE: "",
    _INFO: "INFO: ",
    _ERROR: "ERROR: ",
    _EXTRA: "DEBUG: ",
    _DEBUG: "DEBUG: ",
    _WARNING: "WARNING: "
}

_REQUEST_TIMEOUT = 600

def debug (msg, level = _MESSAGE, json_flag = False):

    global _LOGGER

    if level <= _DEBUG_LEVEL :
        if json_flag:
            log_msg = json.dumps(msg, indent=4, separators=(',', ': '))
            print(log_msg)
            if _LOGGER:
                _LOGGER.info(log_msg)
        else:
            print(_MESSAGE_TEXT[level]+str(msg))
            if _LOGGER:
                _LOGGER.info(_MESSAGE_TEXT[level]+str(msg))
    return None


def get_date_timestamp(current_time=False):
    """
    :param date_format:
        if True returns only YYYY_MM_DD format
        if False returns YYYY_MM_DD_HH_MI_SS format
    :return:
    """
    if not current_time:
        return datetime.datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
    else:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Define Looker API endpoints
LOOKER_API = {
    "LOGIN":("login", requests.post),
    "CREATE_DBCONNECTION":("connections", requests.post),
    "TEST_DBCONNECTION":("connections/{}/test", requests.put),
    "DELETE_DBCONNECTION":("connections/{connection_name}", requests.delete),
    "GET_PROJECT":("projects/{}", requests.get),
    "CREATE_DEPLOY_KEY":("projects/{}/git/deploy_key", requests.post),
    "GET_DEPLOY_KEY":("projects/{}/git/deploy_key", requests.get),
    "UPDATE_PROJECT":("projects/{}", requests.patch),
    "CREATE_LOOKML_MODEL":("lookml_models", requests.post),
    "GET_MODEL_SETS":("model_sets", requests.get),
    "CREATE_MODEL_SET":("model_sets", requests.post),
    "GET_LOOKML_MODELS":("lookml_models", requests.get),
    "UPDATE_LOOKML_MODEL":("lookml_models/{}", requests.patch),
    "DELETE_LOOKML_MODEL": ("lookml_models/{}", requests.delete), 
    "DELETE_MODEL_SETS":("model_sets/{}", requests.delete),
    "CREATE_PERMISSION_SET":("permission_sets", requests.post),
    "DELETE_PERMISSION_SET":("permission_sets/{}", requests.delete),
    "GET_PERMISSION_SETS":("permission_sets", requests.get),
    "CREATE_ROLE":("roles", requests.post),
    "GET_ROLES":("roles", requests.get),
    "UPDATE_SESSION":("session", requests.patch),
    "GET_GROUPS":("groups", requests.get),
    "CREATE_GROUP":("groups", requests.post),
    "GET_ROLE_GROUPS": ("roles/{}/groups?fields=id%2C%20name", requests.get),
    "UPDATE_ROLE_GROUPS":("roles/{}/groups", requests.put),
    "GET_USER_ATTRIBUTES":("user_attributes", requests.get),
    "CREATE_USER_ATTRIBUTE":("user_attributes", requests.post),
    "UPDATE_USER_ATTRIBUTE":("user_attributes/{}", requests.patch),
    "GET_LOOKS": ("looks", requests.get),
    "GET_LOOK": ("looks/{}", requests.get),
    "GET_DASHBOARDS": ("dashboards", requests.get),
    "GET_DASHBOARD": ("dashboards/{}", requests.get),
    "GET_SPACES": ("spaces", requests.get),
    "FIND_SPACE": ("spaces/search?name={}", requests.get),
    "WHO_AM_I": ("user", requests.get),
    "GET_EXPLORE": ("lookml_models/{}/explores/{}", requests.get),
    "RUN_INLINE_QUERY": ("queries/run/{}", requests.post),
    "CREATE_QUERY": ("queries", requests.post),
    "CREATE_LOOK": ("looks", requests.post),
    "CREATE_DASHBOARD": ("dashboards", requests.post),
    "DELETE_DASHBOARD": ("dashboards/{}", requests.delete),
    "CREATE_DASHBOARD_FILTER": ("dashboard_filters", requests.post),
    "CREATE_DASHBOARD_ELEMENT": ("dashboard_elements", requests.post),
    "CREATE_DASHBOARD_LAYOUT": ("dashboard_layouts", requests.post),
    "DELETE_DASHBOARD_LAYOUT": ("dashboard_layouts/{}", requests.delete),
    "UPDATE_DASHBOARD_LAYOUT_COMPONENT": ("dashboard_layout_components/{}", requests.patch),
    "LOGOUT":("logout", requests.delete)

}

# *****************************************************************************
# Function processes properties json file
def get_json_prop(in_json_file):
    """ Args:
                    in_json_file - fileobject returned by argparse
            Returns:
                    Ordered dictionary of properties as defined in passed properties file
       Raises:
            json.decoder.JSONDecodeError: if config file is not valid JSON document
    """
    try:
        out_json_properties = json.load(in_json_file, encoding='utf-8', object_pairs_hook=OrderedDict)
        return out_json_properties
    except json.decoder.JSONDecodeError as json_err:
        debug("Provided config file {} is not valid JSON document".format(in_json_file), _ERROR)
        debug(json_err, _ERROR)
        #raise json_err.with_traceback(sys.exc_info()[2])
        exit(1)
# ************************************

# Complete function - constructs and runs defined API call
def  run_looker_restapi(client_properties, in_access_token,  api_call_name, *api_params, in_payload=None):
    """
    :param client_properties:
    :param in_access_token:
    :param api_call_name:
    :param api_params: optional
    :param in_payload:
    :return: raw response
    """

    header_content = {"Authorization": "token " + in_access_token}

    api_url = get_looker_api_url(client_properties, api_call_name, *api_params)
    if in_payload == None:
        r = LOOKER_API[api_call_name][1](api_url, headers=header_content, verify=False, timeout=_REQUEST_TIMEOUT)
    else:
        r = LOOKER_API[api_call_name][1](api_url, headers=header_content, json = in_payload, verify=False,  timeout=_REQUEST_TIMEOUT)

    return r

# Function constructs and returns Looker REST API URL
def get_looker_api_url(client_properties, api_uri, *params):
    """

    :param client_properties:
    :param api_uri: API request, like login, connections.
    :return:
    """
    looker_api_request = client_properties["api_host"] \
                         + ":" + str(client_properties["api_port"]) \
                         + client_properties["api_endpoint"] \
                         + LOOKER_API[api_uri][0]

    if params:
        looker_api_request = looker_api_request.format(*params)

   # debug("Constructed API URL: {}".format(looker_api_request))
    return looker_api_request

# Function processes Looker REST API request response codes
def get_response_code(in_raw_response):
    """
    :param in_raw_response:
    :return: dictionary in a form {'Response': '<code value>'}
    """
    status_response_code = str(in_raw_response)
    for iter_char in ['<', '>', '[', ']']:
        if iter_char in status_response_code:
            status_response_code = status_response_code.replace(iter_char, '')

    status_response_code_l = status_response_code.split()
    status_response_code_d = dict([(k, v) for k, v in zip(status_response_code_l[::2], status_response_code_l[1::2])])
    status_response_code = int(status_response_code_d["Response"])
    return status_response_code

# Function generates acess token
def get_access_token(client_properties):
    """
    :param client_properties:
    :return: access token in format:
                Authorization: token <generated token>
    """
    payload = {'client_id': client_properties["ClientID"],
           'client_secret': client_properties["ClientSecret"]
           }


    login_url = get_looker_api_url(client_properties, "LOGIN")

    # Construct login request for Access Token.
    # This is the first step in esablishing connection to Looker API

    r = LOOKER_API["LOGIN"][1](login_url, data=payload, verify=False)
    resp_code = get_response_code(r)

    #Process response body
    # Convert stream bytes into json format
    body = r.json()
    if resp_code == 200:
        debug(("Authentication token was obtained successfully"), _INFO)
        access_token = body["access_token"]
        token_expires_in = body["expires_in"]
    else:
        debug("Could not obtain Authentication Token: {}. Aborting deployment".format(body["message"]), _ERROR)
        exit(1)

    debug("Token expires in {} seconds".format(token_expires_in), _INFO)
    return access_token

# Function Logout of the API and invalidate the current access token.
def looker_logout(client_properties, in_access_token):
    """
    :param client_properties: dictionary of client properties from external json file
    :param access token
    :return:
    """
    debug("Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    header_content = {"Authorization": "token " + in_access_token}
    logout_url = get_looker_api_url(client_properties, "LOGOUT")

    r = LOOKER_API["LOGOUT"][1](logout_url, headers=header_content, verify=False)
    resp_code = get_response_code(r)

    if resp_code == 204:
        debug("Successfully logged out of Looker instance", _INFO)
    else:
        body = r.json()
        debug("Could not logout: {}".format(body["message"]))

# Function displays Looker Project properties
def looker_get_project(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """
    _proj_name = client_properties["project_name"]
    r = run_looker_restapi(client_properties, in_access_token, "GET_PROJECT", _proj_name)
    resp_code = get_response_code(r)
    body = r.json()
    if resp_code == 200:
        debug("Project {} properties: {}".format(_proj_name, body))
    else:
        debug("Cannot get project {} properties: {}".format(_proj_name, body["message"]))

# Function switched development modes
def looker_update_session(client_properties, in_access_token, in_workspace_id):
    """
    :param in_access_token:
    :return: True/False
    """
    payload = {"workspace_id": in_workspace_id,  "can": {} }

    r = run_looker_restapi(client_properties, in_access_token, "UPDATE_SESSION", in_payload=payload)
    resp_code = get_response_code(r)
    body = r.json()
    if resp_code == 200:
        debug("Session workspace changed to {}".format(in_workspace_id))
        return True
    elif resp_code == 422:
        debug("Could not change session worspace: {}".format(body["errors"][0]["message"]), _ERROR)
        return False
    else:
        debug("Could not change session worspace: {}".format(body["message"]))
        return False


# Function updates project
def looker_update_project(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :return: True/False
    """
    _proj_name = client_properties["project_name"]
    # Handle offline deployment. For offline deployment git remote url should be set to null
    if client_properties["customer_repo"] is None:
        _git_repo_url = None
    else:
        _git_repo_url = "git@github.com:ModelN/"+client_properties["customer_repo"]+".git"

    looker_update_session(client_properties, in_access_token, in_workspace_id="dev")

    payload = {"git_remote_url": _git_repo_url,
               "git_service_name": client_properties["service_name"]
              }

    r = run_looker_restapi(client_properties, in_access_token, "UPDATE_PROJECT", _proj_name, in_payload=payload)
    resp_code = get_response_code(r)
    body = r.json()
    if resp_code == 200:
        debug("Successfully updated project {}".format(_proj_name), _INFO)
    elif resp_code == 422:
        debug("Cannot update project {}: {}".format(_proj_name, body["errors"][0]["message"]), _ERROR)
    else:
        debug("Cannot update project {}: {}".format(_proj_name, body["message"]), _ERROR)
        debug("Response code {}".format(resp_code))

# Function creates new Looker database connection
def looker_create_dbconnection(client_properties, in_access_token, ClientID):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """
    debug("Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    # Construct connection name based on type of tenant deployment
    if client_properties["single_tenant_deployment"] == 'Y':
        debug("Single tenant db connection format: conn_product_dbtype", _INFO)
        dbconn_name = 'conn_' + client_properties["product_prefix"] + '_' + client_properties["dbconn_db_type"]
    else:
        debug("Multi tenant db connection format: conn_product_ClientID", _INFO)
        dbconn_name = 'conn_' + client_properties["product_prefix"] + '_' + ClientID


    debug("Creating database connection type {}: {}".format(client_properties["dbconn_db_type"], dbconn_name), _INFO)
    payload = {
            "name": dbconn_name,
            "host": client_properties["dbconn_host"],
            "port": client_properties["dbconn_port"],
            "database": client_properties["dbconn_database"],
            "dialect_name": client_properties["dbconn_dialect_name"],
            "username": client_properties["dbconn_username"],
            "password": client_properties["dbconn_user_password"]
        }

    header_content = {"Authorization": "token " + in_access_token}

    create_dbconnection_url = get_looker_api_url(client_properties, "CREATE_DBCONNECTION")
    r = LOOKER_API["CREATE_DBCONNECTION"][1](create_dbconnection_url, headers=header_content, json=payload, verify=False)

    resp_code = get_response_code(r)

    body = r.json()
    if resp_code == 200:
        debug("Database connection {} was successfully created".format(body["name"]), _INFO)
        #return body["name"]
        return dbconn_name
    elif resp_code == 409:
        debug("Could not create connection {}: {}".format(dbconn_name, body["message"]), _WARNING)
        return dbconn_name
    elif resp_code == 422:
        debug("Failed to create connection: {}: {}".format(dbconn_name, body["errors"][0]["message"]), _WARNING)
        return dbconn_name
    else:
        debug("Failed to create connection: {}: {}".format(dbconn_name, body["message"]), _WARNING)
        return dbconn_name


def looker_test_dbconnection(client_properties, in_access_token, db_connection_name):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    header_content = { "Authorization": "token " + in_access_token }

    test_dbconnection_url = get_looker_api_url(client_properties, "TEST_DBCONNECTION", db_connection_name)
    r = LOOKER_API["TEST_DBCONNECTION"][1](test_dbconnection_url, headers=header_content, verify=False)

    resp_code = get_response_code(r)

    body = r.json()

    if resp_code == 200:
        debug("Connection {} was tested successfully".format(db_connection_name), _INFO)
    else:
        debug("Could not test connection {}: {}".format(db_connection_name, body["message"]), _WARNING)

def looker_delete_dbconnection(client_properties, in_access_token, db_connection_name):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """

def looker_get_lookml_models(client_properties, in_access_token, ClientID):
    """
    Function returns
    :param client_properties:
    :param in_access_token:
    :param ClientID
    :return: dictionary of models created for product being deployed - Model Name:Model Label
    """

    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    product_deployed = client_properties["product_prefix"]
    # Get models defined in properties file
    _models = get_application_models(client_properties, CLIENT_PROJECT_DEPLOYMENT_DIR)

    if client_properties["single_tenant_deployment"] == 'N':
        expected_models = [modelIter + '_' + ClientID for modelIter in _models]
    else:
        expected_models =  list(_models)

    # uncomment for debugging
    #debug("Expected models for Product  {}: \n{}".format(product_deployed, '\n'.join(expected_models)), _DEBUG)

    header_content = {"Authorization": "token " + in_access_token}

    get_lookml_models_url = get_looker_api_url(client_properties, "GET_LOOKML_MODELS")
    r = LOOKER_API["GET_LOOKML_MODELS"][1](get_lookml_models_url, headers=header_content, verify=False)
    resp_code = get_response_code(r)

    body = r.json()
    if resp_code == 200:
        # Collect all models available on the instance and returned by REST API call
        # filter only models relevant to a product being deployed and Client
        _configured_oob_models = [l_iter["name"] for l_iter in body if l_iter["name"] in expected_models]

        debug("OOB Models configured on Looker Server - \n{}".format('\n'.join(_configured_oob_models)), _DEBUG)

        # Note: customs for multiple product will show up here, but will be filtered when creating Model Set
        _configured_ps_models = [l_iter["name"] for l_iter in body if re.findall('^(c_)\S+_({0})'.format(ClientID), l_iter["name"])]
        debug("PS Models configured on Looker Server - \n{}".format('\n'.join(_configured_ps_models)))

        # Combine OOB and PS models configured on the server into a single list
        configured_models = _configured_oob_models + _configured_ps_models

        # Create a dictionary in format - Model Name:Model Label
        # filter only models relevant to a product being deployed
        models_product_dict = {d_iter["name"]: d_iter["label"] for d_iter in body if d_iter["name"] in configured_models}
        for key, val in models_product_dict.items():
            debug("SUCCESS: Product {} contains LookML Models - {}:{}".format(product_deployed.upper(), key, val),
                  _DEBUG)

        return models_product_dict
    else:
        debug("Cannot get LookML Models: {}".format(body["message"]), _WARNING)

def looker_create_lookml_model(client_properties,
                               client_proj_deployment_dir,
                               in_access_token,
                               db_connection_name,
                               looker_project_name,
                               *lookml_models):
    """
    :param client_properties:
    :param in_access_token:
    :param lookml_models:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    # Get models from content files
    app_models = get_application_models(client_properties, client_proj_deployment_dir, process_files=True)
    for mi in app_models:
        debug("Add Configuration for model: {}".format(mi), _INFO)

        payload = {
                   "name": mi,
                   "project_name": looker_project_name,
                   "allowed_db_connection_names": [db_connection_name]
                  }
        r = run_looker_restapi(client_properties, in_access_token, "CREATE_LOOKML_MODEL", in_payload=payload)
        resp_code = get_response_code(r)
        body = r.json()
        if resp_code == 200:
            debug("Successfully configured model {}".format(mi), _INFO)
        elif resp_code == 422:
            debug("Cannot configure model {}: {}".format(mi, body["errors"][0]["message"]), _WARNING)
        else:
            debug("Failed to configure model {}: {}".format(body["message"]), _WARNING)
            debug("Check your model configuration", _WARNING)


# Configure Looker Project
# 1. Create Project - manual step
# 2. Generate ssh key for git
# 3. Update Project
def looker_create_deploy_key(client_properties, in_access_token):
    """

    :param client_properties:
    :param in_access_token:
    :return:
    """
    looker_update_session(client_properties, in_access_token, in_workspace_id="dev")
    _project_name = client_properties["project_name"]

    r = run_looker_restapi(client_properties, in_access_token, "CREATE_DEPLOY_KEY", _project_name)

    resp_code = get_response_code(r)

    if resp_code == 200:
        debug("Git SSH Deploy Key for project {} successfully generated.".format(client_properties["project_name"]))
        debug("Copy SSH key for github update")
        ssh_key = r.text


        debug("****************************************")
        debug(ssh_key)
        debug("****************************************")
    else:
        body = r.json()
        debug("Cannot generate deployment key for project {}: {}".format(client_properties["project_name"], body["message"]))

# Function updates LookML Model
def looker_update_model(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """
    debug("*** Updating Looker Models", _INFO)
    db_conn_name = client_properties["product_prefix"] + "_oracle_conn"
    app_models = get_application_models(client_properties)
    debug("Models {} will be updated".format(app_models), _DEBUG)
    for mi in app_models:

        payload = {
                    "project_name": client_properties["project_name"],
                    "allowed_db_connection_names": [db_conn_name],
                    "can": {}
                  }

        r = run_looker_restapi(client_properties, in_access_token, "UPDATE_LOOKML_MODEL", mi,  in_payload=payload)

        resp_code = get_response_code(r)
        body = r.json()
        #debug("Response code: {}".format(resp_code))
        if resp_code == 200:
            debug("Model: {} was updated successfully".format(mi), _INFO)
            #return True
        elif resp_code == 422:
            debug("Could not update model {} - {}".format(mi, body["errors"][0]["message"]), _WARNING)
           # return False
        else:
            debug("Could not update model {} - {}".format(mi, body["message"]), _ERROR)
           # return False

# Function returns all non-built-in model sets available
def looker_get_model_sets(client_properties, in_access_token, looker_models=False, ClientID=False):
    """
    :param: client_properties:
    :param: in_access_token:
    :parameter: dictionary of Looker Models per Product/Customer optional
    :parameter: Cliend ID optional
    :return:
        1. If optional parameters Models and ClientID are defined, returns dictionary in format:
            Model Set Name:[Model Set ID, Related Model Name, Related Model Label]
        2. If no parameters Models and ClientID are defined, returns all Model Sets on the instance
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    _model_set = list()
    _model_sets_dict = defaultdict(list)
    product_deployed = client_properties["product_prefix"]

    r = run_looker_restapi(client_properties, in_access_token, "GET_MODEL_SETS")

    resp_code = get_response_code(r)

    body = r.json()

    if resp_code == 200:
        if looker_models and ClientID:
            debug("Optional parameters: Models and ClientID are defined", _DEBUG)

            # Compile RedEx pattern depending on Product deployed.
            # CDM requires specific handling - only custom models are exposed to end users
            if product_deployed == 'cdm':
                match_model = re.compile('^(c_|base_)\S+(_model_{0})'.format(ClientID))
            else:
                match_model = re.compile('^(c_|base_)\S+(app_model_{0})'.format(ClientID))

            for model_name, model_label in looker_models.items():

                if match_model.match(model_name):
                    debug("Retrieving Model Set for Model {}".format(model_name), _DEBUG)
                    # Construct Model Set name based on Product and Model Name
                    modelset_name = product_deployed.upper() + '_' + model_name
                    _model_set.append(modelset_name)

                    for b_iter in body:
                            if b_iter["name"] == modelset_name:
                                _model_sets_dict[b_iter["name"]].append((b_iter["id"], model_name, model_label))


            for model_set_name, model_set_attr in _model_sets_dict.items():
                debug("MODEL SET NAME/MODEL LABEL - {}:{}".format(model_set_name, list(model_set_attr[0])[2]), _DEBUG)

            # uncomment for debugging
            debug("Expected Model Sets \n{}".format('\n'.join(_model_set)), _DEBUG)

            # uncomment for debugging
            #debug("Model Sets per Models DICT: \n{}".format(model_sets_dict), _DEBUG)

            return _model_sets_dict
        else:
            # Return all Modle Sets configured on Looker instance
            model_sets_dict = {d_iter["name"]: d_iter["id"] for d_iter in body}
            debug("All Model Sets DICT: {}".format(model_sets_dict), _DEBUG)
            return model_sets_dict
    else:
        debug("Cannot get Model Sets: {}".format(body["message"]), _WARNING)

def looker_create_model_set(client_properties, in_access_token, lookml_models, ClientID):
    """
    :parameter client_properties:
    :parameter in_access_token:\
    :parameter lookml_models dictionary
    :parameter ClientID
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    header_content = {"Authorization": "token " + in_access_token}
    product_deployed = client_properties["product_prefix"]

    # Define Model name pattern for creating Model Set
    if product_deployed == 'cdm':
        match_model = re.compile('^(c_|base_)\S+(_model_{0})'.format(ClientID))
        #match_model = re.compile('^(c_)\S+(_model_{0})'.format(ClientID))
    else:
        match_model = re.compile('(c_|base_)\S+(app_model_{0})'.format(ClientID))

    # Iterate through dictionary of models configured on the server for specific product
    for model_name in lookml_models.keys():
        if match_model.match(model_name):
            debug("Creating Model Set for model: {}".format(model_name))

            modelset_name = product_deployed.upper() + '_' + model_name

            payload = {
                "id": 0,
                "name": modelset_name,
                "models": [model_name],
                "built_in": "false",
                "all_access": "true"
            }

            create_model_set_url = get_looker_api_url(client_properties, "CREATE_MODEL_SET")
            r = LOOKER_API["CREATE_MODEL_SET"][1](create_model_set_url, headers=header_content, json=payload, verify=False)

            resp_code = get_response_code(r)
            body = r.json()

            if resp_code == 200:
                debug("Successfully created Model Set: {}".format(body["name"]), _INFO)
            elif resp_code == 422:
                validation_message = body["errors"][0]["message"]
                validation_code = body["errors"][0]["code"]
                if validation_code == 'already_exists':
                    debug("Model Set - {} - already exists and will not be created".format(modelset_name), _INFO)
                else:
                    debug("Cannot create Model Set - {}: {}".format(modelset_name, validation_message), _WARNING)
            else:
                response_message = body["message"]
                debug("Could not create Model Set: {} - {}".format(modelset_name, response_message), _WARNING)

            debug("**************************************************")

def looker_delete_model_set (client_properties, in_access_token, model_set_id=None):
    """
    :param client_properties:
    :param in_access_token:
    :param model_set_id: Model Set ID to be deleted. Optional
    :return:
    """
    # Get a list of all non-built-in Model Sets
    model_sets = looker_get_model_sets(client_properties, in_access_token)
    # Define header
    header_content = {"Authorization": "token " + in_access_token}


    if model_set_id == None:
        debug("The following Model Sets will be removed:".format([l_iter["name"] for l_iter in model_sets]))
        for l_iter in model_sets:
            debug("Deleting Model Sets: {}".format(l_iter["name"]), _INFO)
            # Construct API URL
            delete_model_sets_url = get_looker_api_url(client_properties, "DELETE_MODEL_SETS", l_iter["id"])
            r = LOOKER_API["DELETE_MODEL_SETS"][1](delete_model_sets_url, headers=header_content, verify=False)
            resp_code = get_response_code(r)
            #debug("Delete Model Set response code: {}".format(resp_code), _INFO)


            if resp_code == 204:
                debug("Model Set {} was successfully deleted".format(l_iter["name"]), _INFO)
            else:
                body = r.json()
                debug("Could not delete Model Set: {}: {}".format(l_iter["name"], body["message"]))
    else:
        model_set = [l_iter for l_iter in model_sets if l_iter["id"] == model_set_id]
        debug("Deleting Model Set: {}".format(model_set["name"]))

# Function creates Permission Sets
def looker_create_permission_set(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    product_deployed = client_properties["product_prefix"]
    perm_set_role = client_properties[product_deployed].get("roles")

    if perm_set_role is not None:
        debug("Role {} is defined for Product {}".format(perm_set_role, product_deployed), _INFO)
        #debug(" Permission Sets {} will be created".format(perm_set_role), _INFO)
        permission_sets = client_properties.get("permission_sets")
        #permission_sets = [ps for ps in _permission_sets if ps in perm_set_role]
        #debug(" Permission Sets {} will be created".format(permission_sets), _INFO)

        if permission_sets is not None:
            for ps_name, ps_permissions in permission_sets.items():
                if ps_name not in perm_set_role: continue
                debug("Creating Permission Set: {} \n   With Permissions {}".format(ps_name, ps_permissions), _INFO)
                permset_name = ps_name
                # permset_name = product_deployed.upper() + '_' + ps_name + '_' + ClientID

                payload = {
                    "name": permset_name,
                    "permissions": ps_permissions,
                    "can": {}
                }

                r = run_looker_restapi(client_properties, in_access_token, "CREATE_PERMISSION_SET", in_payload=payload)
                resp_code = get_response_code(r)
                body = r.json()
                # debug("Response code: {}".format(resp_code))
                if resp_code == 200:
                    debug("Successfully created Permission Set: {}".format(permset_name), _INFO)
                elif resp_code == 422:
                    validation_message = body["errors"][0]["message"]
                    validation_code = body["errors"][0]["code"]
                    if validation_code == 'already_exists':
                        debug("Permission Set - {} - already exists and will not be created".format(permset_name), _INFO)
                    else:
                        debug("Cannot create Permission Set - {}: {}".format(permset_name, validation_message), _WARNING)
                elif resp_code == 409:
                    debug("Permission Set {} already exists and will not be created".format(permset_name), _INFO)
                else:
                    response_message = body["message"]
                    debug("Could not create Permission Set {} - {}".format(permset_name, response_message), _WARNING)

        else:
            debug("No Permission Set for Product: {} is defined".format(product_deployed), _WARNING)

    else:
        debug("No Role is defined from Product {}".format(product_deployed), _INFO)

def looker_get_permission_sets(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:

    :return: dictionary of permission sets defined and existing on Looker instance
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    product_deployed = client_properties["product_prefix"]
    perm_set_role = client_properties[product_deployed].get("roles")


    # Get Permission sets defined in config properties
    permission_sets_defined = client_properties.get("permission_sets", "None")

    if permission_sets_defined != "None":
        # Get Permission Sets names only
        #permission_sets_list = list(permission_sets_defined.keys())
        permission_sets_list = [psi for psi in list(permission_sets_defined.keys()) if psi in perm_set_role]

        #debug("Retrieving Permission Sets: {}".format(permission_sets_defined.keys()), _DEBUG)
        debug("List of Permission Sets defined: {} for Product {}".format(permission_sets_list, product_deployed), _DEBUG)
        r = run_looker_restapi(client_properties, in_access_token, "GET_PERMISSION_SETS")
        resp_code = get_response_code(r)
        body = r.json()
        if resp_code == 200:
            permsets_dict = {d_iter["name"]: d_iter["id"] for d_iter in body if d_iter["name"] in permission_sets_list}
            debug("Found Permission Set Name/ID: {}".format(permsets_dict), _DEBUG)
            return permsets_dict
        else:
            debug("Could not retrieve Permission Sets: {}".format(body["message"]), _WARNING)
    else:
        debug("No Permission Set is defined in deployment properties", _WARNING)

    # Create match pattern for Permission Sets name. It can contain blank space
    #match_permset = re.compile('({0})\S+ \S+({1})'.format(product_deployed.upper(), ClientID))

def looker_create_role(client_properties, in_access_token, looker_models, model_sets, permission_sets, ClientID=False):
    """
    :param client_properties:
    :param model_sets:
    :param permission_sets:
    :param ClientID: optional
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Role name pattern: ""ClientID.upper" "Model Label" "Perm Set Name""
    clientID_Upper = ClientID.upper()
    product_deployed = client_properties["product_prefix"]
    roles_to_create = client_properties[product_deployed]["roles"]
    debug(" These Roles will be created: {}".format(roles_to_create), _DEBUG)

    for modelset_name, modelset_attr in model_sets.items():
        #debug("Creating Role - Model Set {}".format(modelset_name), _DEBUG)
        # Get Model Set ID
        modelset_id    = list(modelset_attr[0])[0]
        # Get Model Label related to Model Set
        modelset_label = list(modelset_attr[0])[2]
        for permset_name, permset_id in permission_sets.items():
            #debug("Creating Role - Permission Set {}".format(permset_name), _DEBUG)
            if permset_name in roles_to_create:
                debug("Creating Role for Model Set Name/ID/Label: {}/{}/{} and Perm Set Name/ID: {}/{}".format(modelset_name, modelset_id, modelset_label, permset_name, permset_id), _DEBUG)
                role_name = clientID_Upper + ' ' + modelset_label + ' ' + permset_name

                payload = {
                            "name": role_name,
                            "permission_set_id": permset_id,
                            "model_set_id": modelset_id,
                          }

                r = run_looker_restapi(client_properties, in_access_token, "CREATE_ROLE", in_payload=payload)
                resp_code = get_response_code(r)
                body = r.json()

                if resp_code == 200:
                    debug("Successfully created Role {}".format(role_name), _INFO)
                    #return True
                elif resp_code == 422:
                    validation_message = body["errors"][0]["message"]
                    validation_code = body["errors"][0]["code"]
                    if validation_code == 'already_exists':
                        debug("Role - {} - already exists and will not be created".format(role_name), _INFO)
                    else:
                        debug("Cannot create Role - {}: {}".format(role_name, validation_message), _WARNING)
                    #return False
                elif resp_code == 409:
                    debug("Role {} already exists and will not be created".format(role_name), _INFO)
                else:
                    response_message = body["message"]
                    debug("Could not create Role: {} - {}".format(role_name, response_message), _WARNING)
                    #return False
                debug("************************************************************")


def looker_get_roles(client_properties, in_access_token, ClientID, role_details=False):
    """
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return:
    """
    debug("*** Function call - {} ***".format(sys._getframe().f_code.co_name), _INFO)
    clientID_Upper = ClientID.upper()
    product_deployed = client_properties["product_prefix"]
    roles_defined = client_properties[product_deployed]["roles"]

    if roles_defined:
        r = run_looker_restapi(client_properties, in_access_token, "GET_ROLES")
        resp_code = get_response_code(r)
        body = r.json()

        if resp_code == 200:

            match_role_name = re.compile('^({0}).+'.format(clientID_Upper))
            if not role_details:
                roles_dict = {d_iter["name"]: d_iter["id"] for d_iter in body if match_role_name.match(d_iter["name"])}
                debug("Found Roles Name/ID - {}".format(roles_dict), _INFO)
                return roles_dict
            else:
                debug("Returning Role details", _DEBUG)
                roles_dict = {d_iter["name"]: {"id":d_iter["id"],
                                               "permission_set":{"name":d_iter["permission_set"]["name"]
                                                                },
                                               "model_set":{"name":d_iter["model_set"]["name"],
                                                            "models":d_iter["model_set"]["models"]
                                                           }
                                              } for d_iter in
                              body if match_role_name.match(d_iter["name"])
                             }
                return roles_dict

           # debug("Found Roles Name/ID - {}".format(roles_dict), _INFO)
            debug("************************************************************")
           # return roles_dict
    else:
        debug("No Roles defined in properties", _INFO)

    debug("************************************************************")

def looker_get_groups(client_properties, in_access_token, ClientID, user_groups):
    """
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return: dictionary Group Name:ID for Groups defined in properties
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Group name pattern: "ClientID.upper" "Group name in properties"
    # group_name = clientID_Upper + ' ' + Group Name
    clientID_Upper = ClientID.upper()
    product_deployed = client_properties["product_prefix"]
    if not user_groups:
        groups_defined_list = client_properties[product_deployed]["groups"]
        groups_match_list = [clientID_Upper + ' ' + li for li in groups_defined_list]
    else:
        groups_defined_list = user_groups
        groups_match_list = user_groups

    if groups_defined_list:
        debug("List of Groups defined: {}".format(groups_match_list), _DEBUG)
        r = run_looker_restapi(client_properties, in_access_token, "GET_GROUPS")
        resp_code = get_response_code(r)
        body = r.json()
        if resp_code == 200:
            group_dict = {d_iter["name"]: d_iter["id"] for d_iter in body if d_iter["name"] in groups_match_list}
            debug("Found Group Name/ID: {}".format(group_dict), _DEBUG)
            return group_dict
        else:
            debug("Could not retrieve Groups: {}".format(body["message"]), _WARNING)
    else:
        debug("No Group is defined in deployment properties", _WARNING)

def looker_create_group(client_properties, in_access_token, ClientID, group_name_list):
    """
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Group name pattern: ""ClientID.upper" Group name in properties"
    clientID_Upper = ClientID.upper()
    product_deployed = client_properties["product_prefix"]
    if not group_name_list:
        groups_defined = client_properties[product_deployed]["groups"]
        groups_to_create = [clientID_Upper + ' ' + li for li in groups_defined]
        debug(" Groups will be created based on Product config: {}".format(groups_to_create), _DEBUG)
    else:
        groups_to_create = group_name_list
        debug(" Groups will be created based on User config: {}".format(groups_to_create))

    for gi in groups_to_create:
        group_name = gi
        payload = {
                    "name": group_name,
                  }

        r = run_looker_restapi(client_properties, in_access_token, "CREATE_GROUP", in_payload=payload)
        resp_code = get_response_code(r)
        body = r.json()

        # debug("REST API Response code: {}".format(resp_code), _DEBUG)
        if resp_code == 200:
            debug("Successfully created Group {}".format(group_name))
            # return True
        elif resp_code == 422:
            validation_message = body["errors"][0]["message"]
            validation_code = body["errors"][0]["code"]
            if validation_code == 'already_exists':
                debug("Group - {} - already exists and will not be created".format(group_name), _INFO)
            else:
                debug("Cannot create Group - {}: {}".format(group_name, validation_message), _WARNING)
                # return False
        elif resp_code == 409:
            debug("Group {} already exists and will not be created".format(group_name), _INFO)
        else:
            response_message = body["message"]
            debug("Could not create Group: {} - {}".format(group_name, response_message), _WARNING)
        # return False
        debug("************************************************************")

def looker_get_role_groups(client_properties, in_access_token, in_role_id):
    """
    :param client_properties:
    :param in_access_token:
    :param in_role_id:
    :return: response body - dictionary Group ID:Group Name
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_ROLE_GROUPS", in_role_id)
    resp_code = get_response_code(r)
    body = r.json()
    debug("Getting Group for RoleID {} REST API Response code: {}".format(in_role_id, resp_code), _DEBUG)

    if resp_code == 200:
        debug("Successfully retrieved Groups {} for RoleID {}".format(body, in_role_id), _DEBUG)
        return body
    else:
        response_message = body["message"]
        debug("Could not retrieve Groups for RoleID: {} - {}".format(in_role_id, response_message), _WARNING)

def looker_update_role_groups(client_properties, in_access_token, CliendID, current_roles, current_groups):
    """
    :param client_properties:
    :param in_access_token:
    :param CliendID:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    for role_name, role_id in current_roles.items():
        for group_name, group_id in current_groups.items():
            #debug("Updating Role {} with Group {}".format(role_name, group_name), _DEBUG)

            # Role Name consists of 3 parts: ClientID Upper case + Model Label + Permision Set Name in properties
            match_role  = re.match('^(\w+) (\w+) (.+)', role_name)
            # Group Name consists of two parts: ClientID upper case + Group Name in properties
            match_group = re.match('^(\w+) (.+)', group_name)

            # Note:
            # Roles contain Model Label, Groups don't, so we need to match parts of the Role and Group

            # uncomment for debugging
            #debug("Role Name part {}:{}:{}".format(match_role.group(1), match_role.group(2), match_role.group(3)), _DEBUG)
            #debug("Group Name part {}:{}".format(match_group.group(1), match_group.group(2)), _DEBUG)

            # If Role name and Group name do not correspond to each other - DO NOT update
            if match_role.group(3) != match_group.group(2): continue
            #debug("Updating Role Name/ID: {}/{} with Group Name/ID: {}/{}".format(role_name, role_id, group_name, group_id), _DEBUG )

            get_role_groups = looker_get_role_groups(client_properties, in_access_token, role_id)
            if not get_role_groups:
                debug("No Role/Group assignment exists.", _INFO)
                debug("Safe to update - Role Name/ID: {}/{} with Group Name/ID: {}/{}".format(role_name, role_id, group_name, group_id), _INFO)
                # payload for Role update. contains group_id
                payload = [group_id]
                r_role_update = run_looker_restapi(client_properties, in_access_token, "UPDATE_ROLE_GROUPS", role_id, in_payload=payload)
                resp_code = get_response_code(r_role_update)
                body = r_role_update.json()
                #uncomment for debugging
                #debug("Updating Role with Group REST API Response code: {}".format(resp_code), _DEBUG)

                if resp_code == 200:
                    debug("Successfully updated Role {} with Group {}".format(role_name, group_name), _INFO)
                elif resp_code == 422:
                    validation_message = body["errors"][0]["message"]
                    validation_code = body["errors"][0]["code"]
                    debug("Cannot update Role {} - {}/{}".format(role_name, validation_code, validation_message))
                else:
                    response_message = body["message"]
                    debug("Could not update Role: {} - {}".format(role_name, response_message), _WARNING)

            else:
                debug("Role/Group assignment exists: {}".format(get_role_groups), _WARNING)
                debug("Will not update Role/ID: {}/{} with Group Name/ID: {}/{}".format(role_name, role_id, group_name, group_id), _WARNING)

def looker_update_role_groups_user(client_properties, in_access_token, ClientID, role_id, role_name, group_id_list, group_name):
    """
    :param client_properties:
    :param in_access_token:
    :param CliendID:
    :param role_id:
    :param group_id:
    :return:
    """
    debug("*** Function call - {} ***".format(sys._getframe().f_code.co_name), _INFO)
    debug("**********************************************************************")
    #debug("Role Name/ID: {}/{}".format(role_name, role_id), _DEBUG)
    #debug("Group Name/ID: {}/{}".format(group_name, group_id), _DEBUG)
    payload = group_id_list
    debug("Payload: {}".format(payload), _DEBUG)
    r_role_update = run_looker_restapi(client_properties, in_access_token, "UPDATE_ROLE_GROUPS", role_id,
                                       in_payload=payload)
    resp_code = get_response_code(r_role_update)
    body = r_role_update.json()

    if resp_code == 200:
        debug("Successfully updated Role {} with Group {}".format(role_name, group_name), _INFO)
    elif resp_code == 422:
        validation_message = body["errors"][0]["message"]
        validation_code = body["errors"][0]["code"]
        debug("Cannot update Role {} - {}/{}".format(role_name, validation_code, validation_message))
    else:
        response_message = body["message"]
        debug("Could not update Role: {} - {}".format(role_name, response_message), _WARNING)

def looker_get_user_attributes(client_properties, in_access_token, ClientID):
    """
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return: user attributes defined in deployment properties and exist on Looker for a given product
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    product_deployed = client_properties["product_prefix"]
    user_attributes_expected = client_properties[product_deployed].get("user_attributes", dict()).keys()
    debug("Expected User Attributes: {}".format(user_attributes_expected))

    r = run_looker_restapi(client_properties, in_access_token, "GET_USER_ATTRIBUTES")
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        #debug("User Attributes body for Product: {}".format(product_deployed))
        #debug(body)

        #user_attr_dict = {d_iter["name"]: d_iter["id"] for d_iter in body if d_iter["name"] in user_attributes_expected}
        user_attr_dict = {d_iter["name"]: {"id": d_iter["id"],
                                           "default_value": d_iter["default_value"],
                                           "label":d_iter["label"]
                                          } for d_iter in
                          body if d_iter["name"] in user_attributes_expected
                         }
        debug("Existing User Attributes for Product: {} \n{}".format(product_deployed, user_attr_dict))
        return user_attr_dict

    # _configured_oob_models = [l_iter["name"] for l_iter in body if l_iter["name"] in expected_models]

def looker_create_user_attribute(client_properties, in_access_token, ClientID):
    """
    :parameter client_properties:
    :parameter in_access_token:
    :parameter ClientID
    :return:
    """

    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    clientID_Upper = ClientID.upper()
    product_deployed = client_properties["product_prefix"]
    user_attr_to_create = client_properties[product_deployed].get("user_attributes", "None")
    if user_attr_to_create != "None":

        for user_attr_name, user_attr_config in user_attr_to_create.items():

            _attr_name = user_attr_name
            _attr_label = user_attr_config["label"]
            _attr_data_type = user_attr_config["data_type"]
            _attr_default_value = user_attr_config["default_value"]
            _attr_hide_values = user_attr_config["hide_values"]
            _attr_user_access = user_attr_config["user_access"]

            if _attr_user_access == "view":
                _user_can_view = "true"
                _user_can_edit = "false"
            elif _attr_user_access == "edit":
                _user_can_view = "true"
                _user_can_edit = "true"
            elif _attr_user_access == "none":
                _user_can_view = "false"
                _user_can_edit = "false"
            else:
                debug("Incorect value for User Access property", _ERROR)

            if _attr_hide_values == "yes":
                _hide_values = "true"
            elif _attr_hide_values == "no":
                _hide_values = "false"
            else:
                debug("Incorrect value for Hide Values property", _ERROR)

            payload = {
                          "name": _attr_name,
                          "label": _attr_label,
                          "type": _attr_data_type,
                          "default_value": _attr_default_value,
                          "value_is_hidden": _hide_values,
                          "user_can_view": _user_can_view,
                          "user_can_edit": _user_can_edit
                        }

            r_create_user_attr = run_looker_restapi(client_properties, in_access_token, "CREATE_USER_ATTRIBUTE", in_payload=payload)
            resp_code = get_response_code(r_create_user_attr)
            body = r_create_user_attr.json()

            # debug("REST API Response code: {}".format(resp_code), _DEBUG)
            if resp_code == 200:
                debug("Successfully created User Attribute: {}".format(_attr_name), _INFO)
                debug(" with the following configuration:", _INFO)
                debug("     Name: {}".format(_attr_name), _INFO)
                debug("     Label: {}".format(_attr_label), _INFO)
                debug("     Data Type: {}".format(_attr_data_type), _INFO)
                debug("     User Access: {}".format(_attr_user_access), _INFO)
                debug("     Hide Values: {}".format(_attr_hide_values), _INFO)
                debug("     Default Value: {}".format(_attr_default_value), _INFO)
                # return True
            elif resp_code == 422:
                validation_message = body["errors"][0]["message"]
                validation_code = body["errors"][0]["code"]
                if validation_code == 'already_exists':
                    debug("User Attribute - {} - already exists and will not be created".format(_attr_name), _INFO)
                else:
                    debug("Cannot create User Attribute - {}: {}".format(_attr_name, validation_message), _WARNING)
                    # return False
            elif resp_code == 409:
                debug("User Attribute {} already exists and will not be created".format(_attr_name), _INFO)
            else:
                response_message = body["message"]
                debug("Could not create User Attribute: {} - {}".format(_attr_name, response_message), _WARNING)
            # return False
            debug("************************************************************")

    else:
        debug("Product {} does not require User Attributes configuration".format(product_deployed.upper()), _INFO)

def looker_update_user_attribute(client_properties, in_access_token, in_server_user_attr):
    """
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    product_deployed = client_properties["product_prefix"]
    payload = dict()

    _exist_user_parameters = client_properties.get("update_parameters", "None")
    if _exist_user_parameters != "None":
        user_attr_to_update = _exist_user_parameters.get("user_attributes", "None")

        if user_attr_to_update != "None":

            for user_attr_name, user_attr_config in user_attr_to_update.items():
                if user_attr_name in in_server_user_attr.keys():
                    payload = dict() # reset payload body for next attribute
                    user_attr_id = in_server_user_attr[user_attr_name]["id"]
                    debug("*** User requested update of User Attribute name: {}, ID: {} for Product: {}".format(user_attr_name, user_attr_id, product_deployed.upper()), _INFO)
                    # Iterate through User Attribute configuration
                    for user_attr_config_name, user_attr_config_value in user_attr_config.items():

                        user_attr_config_value_curr = in_server_user_attr[user_attr_name][user_attr_config_name]
                        debug("     Current configuration: {} has value: {}".format(user_attr_config_name,
                                                                                    user_attr_config_value_curr),
                              _INFO)
                        debug("     Updating configuration: {} with value: {}".format(user_attr_config_name,
                                                                                      user_attr_config_value),
                              _INFO)

                        # Prepare payload components Keep all configs for a given attribute in a dictionary
                        payload[user_attr_config_name] = user_attr_config_value

                    # at this point paylaod body is ready for API consumption
                    debug("     Payload: {}".format(payload), _INFO)
                    # Update attribute
                    r_update_user_attr = run_looker_restapi(client_properties, in_access_token, "UPDATE_USER_ATTRIBUTE", user_attr_id,
                                           in_payload=payload)
                    resp_code = get_response_code(r_update_user_attr)
                    body = r_update_user_attr.json()
                    if resp_code == 200:
                        debug("* Successfully updated User Attribute: {}".format(user_attr_name), _INFO)
                    elif resp_code == 422:
                        validation_message = body["errors"][0]["message"]
                        validation_code = body["errors"][0]["code"]
                        debug("Cannot update User Attribute - {}: {} - {}".format(user_attr_name, validation_message, validation_code), _WARNING)
                    else:
                        response_message = body["message"]
                        debug("Could not create User Attribute: {} - {}".format(user_attr_name, response_message), _WARNING)
                else:
                    debug("Attributes for update {} do not match with existing ones {}".format(user_attr_name, list(in_server_user_attr.keys())), _WARNING)
                    debug("Cannot perform User Attribute {} update - it does not exist on the server".format(user_attr_name), _WARNING)

        else:
            debug("No update for User Attributes was requested", _INFO)
    else:
        debug("No object updates were requested", _INFO)


# Function performs initial Customer GitHub content population
def initiate_customer_repository(client_properties, customer_dir):
    """
    :param client_properties:
    :param customer_dir:
    :return:
    """

    prod_repo = client_properties["prod_repo"]
    prod_repo_dir = os.path.join(customer_dir, prod_repo+".git")
    cust_repo = client_properties["customer_repo"]

    # Check if local repository directory exists and remove it
    if os.path.isdir(prod_repo_dir):
        debug("Local Git Repository directory {} exists. It will be removed".format(prod_repo_dir), _INFO)
        shutil.rmtree(prod_repo_dir)

    #"git clone --bare git@github.com:ModelN/revvy-analytics.git --single-branch"
    #"cd revvy-analytics.git"
    #"git push --mirror git@github.com:ModelN/looker-gpm.git"

    git_clone = "git clone --bare git@github.com:ModelN/"+prod_repo+".git --single-branch"
    git_mirror_push = "git push --mirror git@github.com:ModelN/"+cust_repo+".git"

    debug("Cloning Production repo", _INFO)
    subprocess.run(git_clone.split(), timeout=60)
  # TO DO - check for subprocess status before proceed
    os.chdir(os.path.join(customer_dir, prod_repo_dir))
    debug("Mirror push to Customer git", _INFO)
    subprocess.run(git_mirror_push.split(), timeout=60)
  # TO DO - check for subprocess status before proceed

# Update Customer GitHub repository
def update_customer_repository(client_properties):
    """
    Function performs following steps:
    1. Creates full clone of Production repository onto local file system.
    2. Creates full clone of Customer repository onto local file system.
    3. Copies the entire content of Production repository into Customer repository
    4. Runs git commands to add, commit and push new content into Customer repository connected to Looker project
    :param client_properties:
    :return:
    """
    prod_repo = client_properties["prod_repo"]
    cust_repo = client_properties["customer_repo"]

    debug("Syncing Prod repository {} into Customer repository {}".format(prod_repo, cust_repo))

    clone_prod_str = "git clone git@github.com:ModelN/"+prod_repo+".git --single-branch"
    clone_cust_str = "git clone git@github.com:ModelN/"+cust_repo+".git --single-branch"

    subprocess.run(clone_prod_str.split(), timeout=60)
    subprocess.run(clone_cust_str.split(), timeout=60)
    # TO DO - use Popen, implement parallel execution and error handling

def check_prod_apps_models(client_properties):
    """
    :param client_properties:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    expected_products = client_properties["products"]
    product_selected = client_properties["product_prefix"]

    # handle the case when product_apps is not defined
    if client_properties["product_apps"]:
        debug("Found property product_apps: {}".format(client_properties["product_apps"]), _INFO)
        apps_selected = client_properties["product_apps"]
    else:
        debug("Property product_apps is not defined by user", _INFO)
        debug("All applications will be deployed based on internal configuration", _INFO)
        apps_selected = client_properties[product_selected]["apps"]
        #apps_selected = client_properties[product_selected]
        debug(" Applications to be deployed: \n{}".format('\n'.join(apps_selected)), _DEBUG)

    # Check that correct product is defined in client properties
    debug("Checking that Product is defined correctly", _INFO)
    if not product_selected in expected_products:

        debug("Invalid product name {} defined. Check product prefix in properties file.".format(product_selected))
        debug("Possible selections are: {}".format(expected_products), _WARNING)
        debug("Aborting deployment on {}...".format((get_date_timestamp(current_time=True))))
        exit(1)
    else:
        debug("Product selection - {} - is valid".format(product_selected), _INFO)

    # Check that applications selected belong to product selected
    expected_product_apps = client_properties[product_selected]["apps"]

    debug("Possible Application selections: \n{}".format('\n'.join(expected_product_apps)), _DEBUG)
    debug("Applications selected for deployment: \n{}".format('\n'.join(apps_selected)), _DEBUG)

    debug("Checking Applications are correctly defined for product selected.", _INFO)
    found_wrong_apps = set(apps_selected).difference(expected_product_apps)
    if found_wrong_apps:
        debug("Detected wrong application name {}. Check application name in properties file".format(found_wrong_apps), _ERROR)
        debug("Possible application selections for product {} are {}".format(product_selected, expected_product_apps), _WARNING)
        debug("Aborting deployment on {}...".format((get_date_timestamp(current_time=True))))
        exit(1)
    else:
        debug("Applicaitons selection is valid - \n{}".format('\n'.join(apps_selected)), _INFO)


def get_application_models(client_properties, client_proj_deployment_dir, process_files=False):
    """
    :param client_properties:
    :return: list of models
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    product_selected = client_properties["product_prefix"]
    # Function returns list of Models for defined Application(s)
    if not process_files:
        debug("Processing Models based on config property product_apps", _INFO)

        if client_properties["product_apps"]:
            debug("Found user defined property product_apps: {}".format(client_properties["product_apps"]), _INFO)
            debug("Applications will be deployed based on user input", _INFO)
            apps_selected = client_properties["product_apps"]
            debug("Applications to be deployed: {}".format(apps_selected), _INFO)
        else:
            apps_selected = client_properties[product_selected]["apps"]
            #apps_selected = client_properties[client_properties["product_prefix"]]
            debug("Property product_apps is not defined by user", _INFO)
            debug("All applications will be deployed: {}".format(apps_selected), _INFO)

        # Get Models based on Applications selected:
        _app_models = list()
        # Loop through the Applications list and get all related Models
        for app_iter in apps_selected:
            # process application info from internal properties file
            # app_iter is the name of the application from apps_selected list
            _app_models.append(client_properties[app_iter])
            debug("Retrieving Models related to Applications defined: {}".format(app_iter), _INFO)

            # Combine lists of lists into a single list (flatten)
            # and remove duplicates by converting to set.
            app_models = list(set([val for sublist in _app_models for val in sublist]))

        debug("Found Application Models from properties: \n{}".format('\n'.join(app_models)), _INFO)
        return app_models

    else:
        # Process files on Looker Server file system
        debug("Processing Models based on Looker content model files", _INFO)

        _app_models = [f for f in get_files(client_proj_deployment_dir, fpath=False) if re.findall('\S+(model[.]lkml)', f)]
        app_models = [f[0:f.find('.')] for f in _app_models]
        debug("***** Found Application Models from processed content: \n{}".format('\n'.join(app_models)), _DEBUG)
        return app_models

def get_product_view_prefix(client_properties):
    """
    :param client_properties:
    :return:
    """

    product_selected = client_properties["product_prefix"]

    _prod_views_prefix = product_selected + '_views_prefix'
    debug("Product Views Prefix used: {}".format(_prod_views_prefix), _INFO)
    prod_views_prefixes = client_properties[_prod_views_prefix]
    debug("These views prefixes will be used for deployment: {}".format(prod_views_prefixes))
    return prod_views_prefixes

def offline_ps_git_repo_clone(client_properties, client_deployment_dir):
    """
    :param client_properties:
    :param client_deployment_dir:
    :return: ps_repo_location
    """

    debug("Checking PS repository properties", _INFO)
    if client_properties["ps_repo"]:

        ps_repo = client_properties["ps_repo"]

        # Construct git statement to clone from PS master or branch
        if not client_properties["ps_repo_branch"]:
            # Clone from maser
            ps_repo_location = ps_repo
            debug("Cloning PS repository {} from master".format(ps_repo), _INFO)
            clone_ps_str = "git clone git@github.com:ModelN/" + ps_repo + ".git --single-branch"
            if os.path.isdir(os.path.join(client_deployment_dir, ps_repo)):
                debug("PS Repository {} clone exists. Removing it".format(ps_repo))
                shutil.rmtree(ps_repo)
        else:
            # Clone from branch
            ps_branch = client_properties["ps_repo_branch"]
            ps_repo_location = ps_branch
            debug("Cloning PS repository {} from branch {}".format(ps_repo, ps_branch), _INFO)
            clone_ps_str = "git clone -b " + ps_branch + " git@github.com:ModelN/" + ps_repo + ".git " + ps_branch + " --single-branch"
            if os.path.isdir(os.path.join(client_deployment_dir, ps_branch)):
                debug("PS repository {} clone exists. Removing it".format(ps_branch))
                shutil.rmtree(ps_branch)
        debug("Cloning PS repository {}".format(ps_repo), _INFO)
        subprocess.run(clone_ps_str.split(), timeout=60)
        # Check if cloning was successful
        _ps_repo_folder = os.path.join(client_deployment_dir, ps_repo_location)
        if os.path.isdir(_ps_repo_folder):
            debug("PS repository was successfully cloned into folder {}".format(_ps_repo_folder), _INFO)
        else:
            debug("PS repository {} was not cloned".format(ps_repo), _ERROR)
            debug("Aborting deployment", _ERROR)
            exit(1)

        debug("Getting Looker PS content files from: {}".format(ps_repo_location), _INFO)

        # debug("Only files with c_ prefix and related to defined applications will be deployed", _INFO)
        # ps_files = [f for f in get_files(os.path.join(ps_repo_location), fpath=False) if re.search('^(c_)+', f)]
        # debug("Files from PS repository: {}".format(ps_files), _DEBUG)

        return ps_repo_location

    else:
        debug("No PS repository defined. Will not clone and process PS content", _INFO)
        return None
        # End of Process PS repository branch


def offline_oob_git_repo_clone(client_properties, client_deployment_dir):
    """
    :param client_properties:
    :param client_deployment_dir:
    :return: prod_repo_location
    """

    debug("Checking OOB Prod repository properties", _INFO)
    # Process OOB Product branch (mandatory)
    prod_repo = client_properties["prod_repo"]

    # Construct git statement to clone from PROD master of branch
    if not client_properties["prod_repo_branch"]:
        # Clone from master
        prod_repo_location = prod_repo

        debug("Cloning Product OOB repository {} from master".format(prod_repo_location), _INFO)
        clone_prod_str = "git clone git@github.com:ModelN/" + prod_repo_location + ".git --single-branch"
        if os.path.isdir(os.path.join(client_deployment_dir, prod_repo_location)):
            debug("Product Repository {} clone folder exists. Removing it".format(prod_repo_location), _INFO)
            shutil.rmtree(prod_repo_location)
    else:
        # Clone from branch
        prod_branch = client_properties["prod_repo_branch"]
        prod_repo_location = prod_branch

        debug("Cloning Product OOB repository {} from branch {}".format(prod_repo, prod_branch), _INFO)
        clone_prod_str = "git clone -b " + prod_branch + " git@github.com:ModelN/" + prod_repo + ".git " + prod_branch + " --single-branch"
        if os.path.isdir(os.path.join(client_deployment_dir, prod_branch)):
            debug("Product repository {} clone exists. Removing it".format(prod_branch), _INFO)
            shutil.rmtree(prod_branch)

    debug("Cloning Product OOB repository", _INFO)
    subprocess.run(clone_prod_str.split(), timeout=60)
    # Check if cloning was successful
    _prod_repo_folder = os.path.join(client_deployment_dir, prod_repo_location)

    if os.path.isdir(_prod_repo_folder):
        debug("Product repository was successfully cloned into folder {}".format(_prod_repo_folder), _INFO)
    else:
        debug("Product repository {} was not cloned".format(prod_repo_location), _ERROR)
        debug("Aborting deployment", _ERROR)
        exit(1)

    return prod_repo_location

# Function performs offline (no customer github repo) Looker project deployment
def offline_deployment(client_properties,
                       client_deployment_dir,
                       ClientID,
                       db_connection_name
                      ):
    """
    :param - client properties
    :param - client_deployment_dir
    :param - ClientID
    :param - database connection name
    :return:
    """
    debug("Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    debug("*********************************************************************")
    debug("***** Running project deployment in OFFLINE mode", _INFO)
    debug("*********************************************************************")

    global CLIENT_PROJECT_DEPLOYMENT_DIR
    global LOOKER_PROJECT_NAME
    global CONTENT_TARGET_DIR

    # Check if multi tenant deployment is selected
    if client_properties["single_tenant_deployment"] =='Y':
        debug("Single Tenant deployment is selected.", _INFO)
        LOOKER_PROJECT_NAME = client_properties["project_name"]
    else:
        debug("Multi Tenant deployment is selected", _INFO)
        debug("Project, Connections and Models will be renamed", _INFO)
        LOOKER_PROJECT_NAME = ClientID

    CONTENT_TARGET_DIR = os.path.join(client_properties["looker_location"], LOOKER_PROJECT_NAME)

    CLIENT_PROJECT_DEPLOYMENT_DIR = os.path.join(client_deployment_dir, LOOKER_PROJECT_NAME)
    debug("Looker Project Name {}".format(LOOKER_PROJECT_NAME), _INFO)

    if not os.path.isdir(CLIENT_PROJECT_DEPLOYMENT_DIR):
        debug("Client Project Deployment directory does not exist and will be created", _INFO)
        os.mkdir(CLIENT_PROJECT_DEPLOYMENT_DIR)
    else:
        debug("Client Project Deployment directory exists, will back it up", _INFO)
        backup_proj_deployemnt_dir = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR + get_date_timestamp())
        shutil.move(CLIENT_PROJECT_DEPLOYMENT_DIR, backup_proj_deployemnt_dir)
        os.mkdir(CLIENT_PROJECT_DEPLOYMENT_DIR)

#***** Process PS and OOB repositories section.

    # Initialize placeholder for PS and OOB content files. PS content files does not have to exist.
    prod_files = list()

    # Check if COPS specific parameters are present in properties file
    debug("Checking if COPS specific parameters are present in deployment properties file", _INFO)
    if 'prod_repo_local_dir' in client_properties:
        debug(" COPS specific parameter prod_repo_local_dir is present in client properties file", _INFO)
        # COPS pre-cloned Looker content
        prod_repo_local = client_properties["prod_repo_local_dir"]

        if 'ps_repo_local_dir' in client_properties:
            debug(" COPS specific parameter ps_repo_local_dir is present in client properties file", _INFO)
            ps_repo_local = client_properties["ps_repo_local_dir"]
        else:
            debug(" COPS specific parameter ps_repo_local_dir is NOT present in client properties file", _INFO)

        # Process COPS pre-cloned folders - if defined, take the highest priority
        if prod_repo_local:
            debug("COPS pre-cloned Looker OOB Prod content exists", _INFO)
            debug(" Only COPS pre-cloned content will be processed", _INFO)
            prod_repo_location = prod_repo_local
            debug("COPS pre-cloned OOB Prod location: {}".format(prod_repo_location), _INFO)

            if ps_repo_local:
                debug("COPS pre-cloned Looker PS content exists")
                ps_repo_location = ps_repo_local
                debug(" COPS pre-cloned PS location {}".format(ps_repo_location), _INFO)

            else:
                debug(" COPS pre-cloned Looker PS content does not exist", _INFO)
                ps_repo_location = None

        else:

            debug("COPS pre-cloned content DOES NOT exist", _INFO)
            debug("Repositories will be cloned by deployment app", _INFO)

            # Process PS repository branch. It is optional.
            ps_repo_location = offline_ps_git_repo_clone(client_properties, client_deployment_dir)

            prod_repo_location = offline_oob_git_repo_clone(client_properties, client_deployment_dir)

    else:
        debug(" COPS specific parameter prod_repo_local_dir is NOT present in client properties file")
        debug(" Repositories will be cloned by deployment app", _INFO)

        ps_repo_location = offline_ps_git_repo_clone(client_properties, client_deployment_dir)
        prod_repo_location = offline_oob_git_repo_clone(client_properties, client_deployment_dir)


    # Collect OOB and PS content section
    # 1. Collect optional PS content
    replacement_tokens_file_name = client_properties["replacement_tokens_file_name"]
    # initialize placeholders for OOB and PS replacement tokens content
    _ps_replacement_tokens = dict()
    _oob_replacement_tokens = dict()

    if ps_repo_location is not None:
        debug("Only files with c_ prefix and *.json files will be deployed", _INFO)
        # Include the following files:
        # 1. All files starting with c_
        # 2. All files which have extension .json
        # 3. All Model files containing token "map". Need to re-evaluate this
        #ps_files = [f for f in get_files(os.path.join(ps_repo_location), fpath=False) if re.search('(^(c_).+|\S+.json|\S+map\S+model[.]lkml)', f)]
        match_ps_content = re.compile(r'(^(c_).+|\S+.json|\S+map\S+model[.]lkml)')
        ps_files = [f for f in get_files(os.path.join(ps_repo_location), fpath=False) if match_ps_content.search(f)]

        debug("Files from PS repository:\n{}".format('\n'.join(ps_files)), _DEBUG)

        # Need to check if custom tokens were defined for replacement
        ps_replacement_tokens_file = os.path.join(client_deployment_dir, ps_repo_location+'/'+replacement_tokens_file_name)

        if os.path.isfile(ps_replacement_tokens_file):
            debug("Found PS tokens file: {}. Will use it for Custom tokens replacement".format(ps_replacement_tokens_file), _INFO)
            # open file for reading
            ps_replacement_tokens_file_fh = open(ps_replacement_tokens_file, 'r')
            _ps_replacement_tokens = get_json_prop(ps_replacement_tokens_file_fh)
        else:
            debug("No PS replacement tokens file {}. Will use OOB config for tokens replacement".format(replacement_tokens_file_name), _INFO)

    else:
        debug("There is no PS Looker content files", _INFO)
        # Initialise empty list to avoid more complex checks later in the code.
        # Empty ps_files list means there is no PS content to deploy
        ps_files = list()

    # 2. Collect all OOB Product content
    debug("Getting OOB Looker content files from: {}".format(prod_repo_location), _INFO)
    prod_files = get_files(os.path.join(prod_repo_location), fpath=False)

    oob_replacement_tokens_file = os.path.join(client_deployment_dir, prod_repo_location+'/'+replacement_tokens_file_name)
    if os.path.isfile(oob_replacement_tokens_file):
        debug("Found OOB tokens file: {}. Will use it for Custom tokens replacement".format(oob_replacement_tokens_file), _INFO)
        oob_replacement_tokens_file_fh = open(oob_replacement_tokens_file, 'r')
        _oob_replacement_tokens = get_json_prop(oob_replacement_tokens_file_fh)
    else:
        debug("No OOB replacement tokens file {}. Token replacement will not be performed".format(replacement_tokens_file_name), _INFO)

    # Combine PS and OOB content into a single object for unified processing
    all_content_files = ps_files + prod_files
    #uncomment for debugging
    #debug("Combined content files: {}\n".format(all_content_files), _DEBUG)

#***** End of Process PS and OOB repositories section.

    debug("Getting Models for initial Model files processing", _INFO)
    app_models = get_application_models(client_properties, CLIENT_PROJECT_DEPLOYMENT_DIR)

    # Process Model files.
    debug("*********************************************************************")
    debug(" *****  Processing Looker Model files...", _INFO)

    # Initialize lists to separately hold processed OOB and Custom model files.
    # need it for renaming
    _model_files = list()
    _custom_model_files = list()

    # Need this list for processing models in view/document files.
    old_model_name = list()

    # Create list to hold application tokens processed. We need it for PS content validation
    app_tokens = list()

    for mi in app_models:
        debug("Processing Model: {}".format(mi), _INFO)
        debug("*********************************************************************")

        # ***** obsolete section. Need to remove and re-test
        # first part of IF handles Custom Models developed by PS and defined in internal properties file
        if re.search('^(c_)\S+', mi):
            debug("Found Customized Model: {}".format(mi))
            #shutil.copy2(os.path.join(all_content_files, mi + ".model.lkml"), CLIENT_PROJECT_DEPLOYMENT_DIR)
            shutil.copy2(os.path.join(ps_repo_location, mi + ".model.lkml"), CLIENT_PROJECT_DEPLOYMENT_DIR)
        else:
            debug("Process Model files based on application token extracted from application name", _INFO)
    # ***** end of obsolete section

        # process model files based on application token extracted from application name.
        # this part processes OOB and PS content solely based on app-model token extracted
        # from string base_<app-model-token>_model
            _model_pos = mi.find('model')
            _base_pos = mi.find('base')

                # uncomment for debugging token extraction
            #debug(" base pos {}, model pos {} ".format(_model_pos, _base_pos), _DEBUG)
            if _model_pos < 0 or _base_pos < 0: # common models don't have _model prefix
                debug("This is Common Model intended for extension", _INFO)
                app_model_token = mi
                app_tokens.append(app_model_token)
            else:
                app_model_token = mi[_base_pos + len('base') + 1:_model_pos - 1]
                app_tokens.append(app_model_token)
            debug("Extracted app-model token: {}".format(app_model_token), _DEBUG)

            debug("Processing Model files from PS repository based on app-model token: {}".format(app_model_token), _INFO)
            debug(" There might not be any PS Model files as PS content is optional", _INFO)
            for fi in ps_files:
                #if re.search('^(c_\S*{0}|\S+map)\S+model[.]lkml'.format(app_model_token), fi):
                if re.search('^(c_\S*{0})\S+model[.]lkml'.format(app_model_token), fi):
                    debug(" Copying PS Model file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                    shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
                    # Collect model files names for renaming if needed
                    _custom_model_files.append(fi)

                # Search for PS Model files files without app token (negative lookahead)
                if re.search('^((?!{0}).)*model[.]lkml$'.format(app_model_token), fi):
                    debug("Found PS Model file {} without app token: {}".format(fi, app_model_token))
                    shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
                    # List might contain duplicates due to multiple negative lookahead matches
                    _custom_model_files.append(fi)
            # Remove duplicates resulting from multiple negative (not matching token) matches while iterating through app tokens
            dup_values= set()
            custom_model_files = [x for x in _custom_model_files if x not in dup_values and not dup_values.add(x)]

            debug("Processing Model files from Prod repository based on app-model token: {}".format(app_model_token), _INFO)
            for fi in prod_files:
                if re.search('\S*({0})\S*[.]model[.]lkml'.format(app_model_token), fi):
                    debug(" Copying OOB Prod model file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                    shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
                    # Collect model file names to be renamed and parsed for connection
                    _model_files.append(fi)
            # Dedup OOB model file names
            dup_values= set()
            model_files = [x for x in _model_files if x not in dup_values and not dup_values.add(x)]

        debug("*********************************************************************")

    debug("Application tokens processed: {}".format(app_tokens), _INFO)

    debug("Completed Model files processing", _INFO)
    debug("*********************************************************************")
    debug("*********************************************************************")

    # Rename model files for every new Customer deployment if needed
    model_file_extension = '.model.lkml'
    hide_oob_model_explores = client_properties.get("hide_oob_explores","N")
    replace_hide_explore_condition = 'hidden: yes'
    replace_db_conn_name = 'connection: "' + db_connection_name + '"'

    if client_properties["single_tenant_deployment"] =='Y':
        debug("Deployment is in Single tenant mode. Will rename connections only", _INFO)
        debug("Connection name: {}".format(db_connection_name), _INFO)

        debug("Renaming connections for models: \n {}".format(model_files), _INFO)
			
        for fi in model_files:
			# Search for OOB model files extended by PS to hide the explores in OOB model
            hide_explores_in_model = 0
            prod_file = fi[5:]
            for ps_fi in ps_files:
                ps_file = ps_fi[2:]
                if prod_file == ps_file:
                    hide_explores_in_model = 1
                    ps_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, ps_fi)
                    break

            model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
            debug("     Processing file {} for connection rename in Single Tenant mode".format(model_file_name), _INFO)
            if (hide_oob_model_explores == 'Y') and (hide_explores_in_model == 1):			
                debug("     Hiding OOB explore(s) exists in {} model file as it is extended by {}".format(model_file_name,ps_model_file_name), _INFO)
			
            with fileinput.input(files=(model_file_name), inplace=True) as modf:
                for line in modf:
                    if (hide_oob_model_explores == 'Y') and (hide_explores_in_model == 1) :
                        if line.lstrip().rstrip() == 'hidden: no':
                            match_conn_string = re.sub(r'hidden: no', replace_hide_explore_condition, line)
                        else:
                            match_conn_string = re.sub(r'^connection[:].+', replace_db_conn_name, line)
                    else:
                        match_conn_string = re.sub(r'^connection[:].+', replace_db_conn_name, line)

                    print(match_conn_string, end='')

    # processing for Multi Tenant mode
    else:
        renamed_model_files = list()

        debug("Deployment is in Multi tenant mode.", _INFO)
        debug("These OOB Model files will be renamed: \n {}".format('\n'.join(model_files)), _INFO)
        # Rename OOB base model files and replace connection string

        for fi in model_files:
            debug("Processing OOB Model file {} in Multi Tenant mode".format(fi), _INFO)
			
            old_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
            file_part = fi.split('.')[0]
            old_model_name.append(file_part)

            new_model_file_name_part = file_part + '_' + ClientID + model_file_extension

            debug("OOB Model file name without extension: {}".format(file_part), _DEBUG)
            new_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, new_model_file_name_part)
            try:
                debug("Renaming OOB Model file {} to {}".format(old_model_file_name, new_model_file_name), _INFO)
                os.rename(old_model_file_name, new_model_file_name)
            except OSError as e:
                debug("Cannot rename old file: {} to \n new: {}".format(old_model_file_name, new_model_file_name), _ERROR)
                debug("Error: {}".format(str(e)), _ERROR)
                debug("Traceback: {}".format(traceback.format_exc()))
                exit(1)

            # Collect renamed OOB Model file names for processing extended models
            renamed_model_files.append(new_model_file_name_part)

            # Process connection string in model file
            debug("Renaming connections and extended OOB Model file names in file {}".format(new_model_file_name), _INFO)
            debug(" New connection name: {}".format(replace_db_conn_name))
            
            debug("Search for OOB model files extended by PS to hide the explores in OOB model", _DEBUG)
            hide_explores_in_model = 0 
            prod_file = new_model_file_name_part[5:]
            debug("Checking PROD FILE {} for hiding models ".format(prod_file), _DEBUG)
            for ps_fi in ps_files:
                ps_file_part = ps_fi[2:].split('.')[0]			
                ps_file = ps_file_part + model_file_extension
                debug("Checking against PS FILE {}".format(ps_file), _DEBUG)
                if prod_file == ps_file:
                    hide_explores_in_model = 1
                    ps_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, ps_fi)
                    break

            if (hide_oob_model_explores == 'Y') and (hide_explores_in_model == 1):			
                debug("Hiding OOB explore(s) exists in {} model file as it is extended by {}".format(new_model_file_name,ps_model_file_name), _INFO)
				
            with fileinput.input(files=(new_model_file_name), inplace=True) as modf:
                for line in modf:
                    if (hide_oob_model_explores == 'Y') and (hide_explores_in_model == 1) :
                        if line.lstrip().rstrip() == 'hidden: no':
                            match_conn_string = re.sub(r'hidden: no', replace_hide_explore_condition, line)
                        else:
                            match_conn_string = re.sub(r'^\s*connection[:].+', replace_db_conn_name, line)
                    else:
                        match_conn_string = re.sub(r'^\s*connection[:].+', replace_db_conn_name, line)

                    print(match_conn_string, end='')

        debug("Renamed OOB Model files: \n {}".format('\n'.join(renamed_model_files)), _INFO)
        # Rename extended models in renamed model files
        debug("Renaming OOB extended model file names in OOB Model files", _INFO)
        for fi in renamed_model_files:
            model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
            debug(" Processing OOB Model file {} for OOB extended model renaming".format(fi), _INFO)
            with fileinput.input(files=(model_file_name), inplace=True) as modf:
                for line in modf:
                    match_extened_model_string = re.sub(r'^\s*include.+[.]model[.]lkml', line.split('.')[0] + '_' + ClientID + model_file_extension, line)
                    print(match_extened_model_string, end='')

        # Process and rename Custom Model files if required
        rename_cust_models = client_properties.get("existing_model_deployment_id", None)
        if rename_cust_models is not None:
            existing_model_deployment_id = client_properties["existing_model_deployment_id"]
            if existing_model_deployment_id and custom_model_files:
                # Initialize list for renamed custom Model files. Need it for renaming referenced models
                renamed_custom_model_files = list()

                debug("Custom Models will be renamed:", _INFO)
                debug(" Existing deployment id _{} will be replaced with _{}".format(
                    existing_model_deployment_id, ClientID), _INFO)
                debug("Custom Model files to be renamed: {}".format(custom_model_files), _DEBUG)
                for cfi in custom_model_files:
                    debug("Processing Custom Model file {} in Multi Tenant mode".format(cfi), _INFO)
                    file_part = cfi.split('.')[0]
                    debug("Custom Model file part: {}".format(file_part), _DEBUG)

                    # Check if file part contains existing custom model token or not
                    match_model_file_part = re.compile(r'_({0})$'.format(existing_model_deployment_id))
                    if match_model_file_part.search(file_part):
                        debug("Custom Model file {} contains deployment id: {}".format(file_part, existing_model_deployment_id), _DEBUG)

                        new_custom_model_file_name_part = file_part.replace(existing_model_deployment_id, ClientID) + model_file_extension
                        debug("New Custom Model file name: {}".format(new_custom_model_file_name_part), _DEBUG)
                        old_custom_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, cfi)
                        new_custom_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, new_custom_model_file_name_part)
                        debug("Renaming Custom Model file {} to {}".format(old_custom_model_file_name, new_custom_model_file_name), _INFO)
                        os.rename(old_custom_model_file_name, new_custom_model_file_name)
                        # Collect renamed Custom Model files for renaming referenced models
                        renamed_custom_model_files.append(new_custom_model_file_name)
                    else:
                        debug("Custom Model file {} has no deployment id: {}".format(file_part, existing_model_deployment_id), _DEBUG)

                        new_custom_model_file_name_part = file_part + '_' + ClientID + model_file_extension
                        debug("New Custom Model file name: {}".format(new_custom_model_file_name_part), _DEBUG)
                        old_custom_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, cfi)
                        new_custom_model_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, new_custom_model_file_name_part)
                        debug("Renaming Custom Model file {} to {}".format(old_custom_model_file_name, new_custom_model_file_name), _INFO)
                        os.rename(old_custom_model_file_name, new_custom_model_file_name)
                        # Collect renamed Custom Model files for renaming referenced models
                        renamed_custom_model_files.append(new_custom_model_file_name)

                    debug("********************************************************************************")

                # Rename referenced model files
                debug("Process in-file references:: Renaming OOB extended Model files in Custom Model files", _INFO)
                for rfi in renamed_custom_model_files:
                    debug(" Processing Custom Model file {} for OOB extended model renaming".format(rfi), _INFO)
                    match_ext_cust_model = re.compile(r'(^\s*include:.+_)({0})([.]model[.]lkml)'.format(existing_model_deployment_id))
                    with fileinput.input(files=(rfi), inplace=True) as cmodf:
                        for line in cmodf:
                            print(match_ext_cust_model.sub(r'\1{0}\3'.format(ClientID), line), end='')

                debug("Renaming connections in Custom Model files", _INFO)
                for rfi in renamed_custom_model_files:
                    debug(" Processing Custom Model file {} for connection renaming".format(rfi), _INFO)
                    with fileinput.input(files=(rfi), inplace=True) as cmodf:
                        for line in cmodf:
                            match_conn_string = re.sub(r'^\s*connection[:].+', replace_db_conn_name, line)
                            print(match_conn_string, end='')

                # Rename referenced models without deployment id
                for rfi in renamed_custom_model_files:
                    debug(" Processing Custom Model file {} for renaming Custom extended model without deployment id".format(rfi), _INFO)
                    match_ext_cust_model = re.compile('^((?!{0}).)*model[.]lkml$'.format(existing_model_deployment_id))
                    with fileinput.input(files=(rfi), inplace=True) as cmodf:
                        for line in cmodf:
                            match_no_dep_id = re.sub('^\s*include:\s\".*(?<!{0})\.model\.lkml\"$'.format(ClientID),
                                                     line.split('.')[0] + '_' + ClientID + model_file_extension + '"',
                                                     line)
                            print(match_no_dep_id, end='')

            else:
                debug("Custom Model files will not be renamed", _INFO)
        else:
            debug(
                "Parameter existing_model_deployment_id is not defined. Custom Models won't be renamed", _INFO)

    # Process Dashboard and Document files based on Model name
    debug("*********************************************************************")
    debug("***** Processing Dashboard and Document files", _INFO)
    document_files = list()
    for mi in app_models:
        debug("Processing files for Model: {}".format(mi), _DEBUG)
        debug("Extract token from Model name for Dashboard name matching", _INFO)

        _model_pos = mi.find('model')
        _base_pos = mi.find('base')
        # ignore models with no dashboards - common models (base_cpq_clm_base_explores)
        # they do not contain _model suffix, only base_
        if _model_pos < 0: continue

        dashboard_document_token = mi[_base_pos + len('base') + 1:_model_pos - 1]

        # Uncomment below debug commands for troubleshooting
        debug("Dashboard token - {}".format(dashboard_document_token), _DEBUG)
        debug("Document token - {}".format(dashboard_document_token), _DEBUG)

        debug("Processing PS dashboards and documents based on c_ prefix", _INFO)
        debug("There might not be any PS files as PS content is optional", _INFO)
        for fi in all_content_files:
            #if re.search('^c_({0})\S+dashboard[.]lookml'.format(dashboard_document_token), fi):
            if re.search('^c_\S+[.]dashboard[.]lookml', fi):
                debug(" Copying PS dashboard file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

            if re.search('^c_\S+readme[.]md'.format(dashboard_document_token), fi):
                debug(" Copying PS document file: {} to ".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)

                document_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
                document_files.append(document_file_name)
                shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

        debug("Processing OOB dashboards and documents based on token: {}".format(dashboard_document_token), _INFO)
        for fi in all_content_files:
            if re.search('^(base_\S*{0}\S+)dashboard[.]lookml'.format(dashboard_document_token), fi):
                debug(" Copying OOB Prod dashboard file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR))
                shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

            if re.search('^({0})[_]readme[.]md'.format(dashboard_document_token), fi):
                debug(" Copying OOB Prod document file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)

                document_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
                document_files.append(document_file_name)
                shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

        debug("*********************************************************************")

    # Search through document files for dashboard links and replace model names
    if client_properties["single_tenant_deployment"] == 'N':
        debug("Search document files for dashboard links and replace model names", _INFO)
        for dfi in document_files:
            for dmodelIter in old_model_name:
                debug("Searching Document file {} for model name {}".format(dfi, dmodelIter), _DEBUG)
                replace_model_name = dmodelIter + '_' + ClientID
                match_model_name = re.compile(r'(.*[/]dashboards[/])({0})(([/]|::)\S+)'.format(dmodelIter))
                debug(" Replacing model: {} with new model: {}".format(dmodelIter, replace_model_name), _DEBUG)
                with fileinput.input(files=(dfi), inplace=True) as modf:
                    for line in modf:
                        print(match_model_name.sub(r'\1{0}\3'.format(replace_model_name), line), end='')

    # Copy View files
    debug("*********************************************************************")
    debug("*** Processing Views files based on Product {} view prefix".format(client_properties["product_prefix"]))
    view_files = list()
    prod_views_prefix = get_product_view_prefix(client_properties)
    for vi in prod_views_prefix:

        debug("Processing views with prefix {} from PS content".format(vi), _INFO)
        debug("There might not be any PS files as PS content is optional", _INFO)
        for fi in all_content_files:

            if re.search('^c_\S+[.]view[.]lkml', fi):
                debug("Copying PS view file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

        debug("Processing views with prefix {} from OOB PROD content".format(vi), _INFO)
        for fi in all_content_files:
            if re.search('^base_({0})\S+view[.]lkml'.format(vi), fi):

                debug("Copying OOB Prod view file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                view_file_name = os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR, fi)
                view_files.append(view_file_name)
                # Copy view files
                shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

    debug("Content is copied into folder {}".format(CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
    debug("*********************************************************************")

    # Search through view files for dashboard links and replace model names
    if client_properties["single_tenant_deployment"] == 'N':
        debug("Search view files for dashboard links and replace model names", _INFO)

        for fi in view_files:
            for modelIter in old_model_name:
                # uncomment for debugging
                #debug("Searching view file {} for model name {}".format(fi, modelIter), _DEBUG)
                replace_model_name = modelIter + '_' + ClientID
                match_model_name = re.compile(r'(.*[/]dashboards[/])({0})(([/]|::)\S+)'.format(modelIter))
                #debug(" Replacing model: {} with new model: {}".format(modelIter, replace_model_name), _DEBUG)
                with fileinput.input(files=(fi), inplace=True) as modf:
                    for line in modf:
                        print(match_model_name.sub(r'\1{0}\3'.format(replace_model_name), line), end='')

        debug("Completed view files search and replacement.", _INFO)
    debug("*********************************************************************")

	# Copy Visualization Extension files
    debug("*** Processing Visualization Extension files for Product {} ".format(client_properties["product_prefix"]))
    app_deployment_base = client_properties.get("looker_deployment_base")
    product_deployed = client_properties["product_prefix"]
    visual_ext_files = client_properties[product_deployed].get("visual_ext_files")

    d3_files_location = client_properties.get("d3_files_location")
    if d3_files_location is None:
        debug("Parameter d3_files_location does not exist in internal properties file", _WARNING)
        d3_files_dir = "foo"
    else:
        d3_files_dir = os.path.join(app_deployment_base, d3_files_location)

    if os.path.isdir(d3_files_dir):
        debug("Get Visualisation extension files from central location: {}".format(d3_files_dir), _INFO)
        if visual_ext_files is None:
            debug("All Visual Extension files will be deployed for Product: {}".format(product_deployed), _INFO)
            d3_files = get_files(os.path.join(d3_files_dir), fpath=False)
        else:
            debug("Selected Visual Extension files will be deployed for Product: {}".format(product_deployed), _INFO)
            debug('\n'.join(visual_ext_files))
            _d3_files = get_files(os.path.join(d3_files_dir), fpath=False)
            d3_files = [f for f in _d3_files if f in visual_ext_files]

        #debug("Visualization files: \n{}".format('\n'.join(d3_files)))
        #debug("Copying Visualisation extension files from {} to {}".format(d3_files_dir, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
        for fi in d3_files:
            if re.search('\S+[.]js$', fi):
                debug("Copying Visualization extension file: {} from {} to {}".format(fi, d3_files_dir, CLIENT_PROJECT_DEPLOYMENT_DIR))
                shutil.copy2(os.path.join(d3_files_dir, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
    else:
        debug("Get Visualisation extension files from PD repo location: {}".format(prod_repo_location))
        debug("Visualization files .js might not exist", _INFO)
        for fi in all_content_files:
            if re.search('\S+[.]js$', fi):
                debug("Copying Visualization extension file: {} from {} to {}".format(fi, prod_repo_location, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
                shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)

    # Copy other files - e.g Looker topojson file for various dashboards with maps
    debug("*********************************************************************")
    debug("***** Processing topo json files", _INFO)
    topojson_files = list()
    debug("Processing content from PS repository", _INFO)
    debug(" There might not be any PS files as PS content is optional", _INFO)
    for fi in ps_files:
        if re.search('\S+.json$', fi):
            debug(" Copying PS json file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
            shutil.copy2(os.path.join(ps_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
            topojson_files.append(fi)

    debug("Processing content from OOB repository", _INFO)
    for fi in prod_files:
        if re.search('\S+.json$', fi):
            debug(" Copying OOB json file: {} to {}".format(fi, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
            shutil.copy2(os.path.join(prod_repo_location, fi), CLIENT_PROJECT_DEPLOYMENT_DIR)
            topojson_files.append(fi)

    debug("TOPO json files {} are copied into folder {}".format(topojson_files, CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
    debug("*********************************************************************")

    debug("Content is copied into folder {}".format(CLIENT_PROJECT_DEPLOYMENT_DIR), _INFO)
    debug("*********************************************************************")

    # Replace token in model and dashboard files
    # before replacing tokens we need to choose whcih config to use: OOB or PS

    if _ps_replacement_tokens:
        debug("Performing Looker content customization with PS tokens", _INFO)
        replacement_tokens = copy.deepcopy(_ps_replacement_tokens)
        match_replace_token(client_properties, replacement_tokens, CLIENT_PROJECT_DEPLOYMENT_DIR)
    elif _oob_replacement_tokens:
        debug("Performing Looker content replacement with OOB tokens", _INFO)
        replacement_tokens = copy.deepcopy(_oob_replacement_tokens)
        match_replace_token(client_properties, replacement_tokens, CLIENT_PROJECT_DEPLOYMENT_DIR)
    else:
        debug("Product {} does not require content replacement, no OOB/PS tokens".format(product_deployed), _INFO)


    debug("Ready to propagate content to Looker server", _INFO)

    # if client_properties["single_tenant_deployment"] == 'Y':
    #     looker_project_name = client_properties["project_name"]
    # else:
    #     looker_project_name = ClientID

    # CONTENT_TARGET_DIR = os.path.join(client_properties["looker_location"], looker_project_name)

    debug("Copying content into folder {}".format(CONTENT_TARGET_DIR), _INFO)

    # Copy prepared content under Looker models folder.
    combined_content = get_files(os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR), fpath=True)
    visualization_extn_dir = client_properties.get("looker_viz_extn_location","None")

    # TO DO check if CONTENT_TARGET_DIR exists before processing content
    if visualization_extn_dir != "None":
        try:
            os.makedirs(visualization_extn_dir, exist_ok=True)
        except OSError as e:
            debug("Cannot create Visualization extensions folder {}".format(visualization_extn_dir), _ERROR)
            debug("Error: {}".format(str(e)), _ERROR)
            debug("Traceback: {}".format(traceback.format_exc()))
            exit(1)
    else:
        debug("Visualization extension directory is not defined", _WARNING)

    # Check if Looker Project directory exists, create empty directory if it does not
    if os.path.isdir(CONTENT_TARGET_DIR):
        for fi in combined_content:
            if not (re.search("\S+[.]js$", fi)):
                content_target_final_dir = CONTENT_TARGET_DIR
                try:
                    shutil.copy2(fi, content_target_final_dir)
                except IOError:
                    debug("Unable to copy file {} to folder {}".format(fi, content_target_final_dir), _ERROR)
            else:
                # Vizualization extensions files exist
                debug("Vizualization extension file {} exists".format(fi), _INFO)
                if visualization_extn_dir != "None":
                    debug("Vizualization extension directory is defined: {}".format(visualization_extn_dir), _DEBUG)

                    content_target_final_dir = visualization_extn_dir
                    debug("Copying Visualization extension file {} into folder {}".format(fi, content_target_final_dir), _INFO)

                    try:
                        shutil.copy2(fi, content_target_final_dir)
                    except IOError:
                        debug("Unable to copy file {} to folder {}".format(fi, content_target_final_dir), _ERROR)
                else:
                    #content_target_final_dir = CONTENT_TARGET_DIR
                    debug("Visualization extension directory is not defined", _WARNING)
                    debug("Visualization extension file {} will not be copied".format(fi), _WARNING)

    else:
        debug("Folder {} does not exist. Creating it".format(CONTENT_TARGET_DIR), _INFO)
        os.mkdir(CONTENT_TARGET_DIR)
        for fi in combined_content:
            if not (re.search("\S+[.]js$", fi)):
                content_target_final_dir = CONTENT_TARGET_DIR
                try:
                    shutil.copy2(fi, content_target_final_dir)
                except IOError:
                    debug("Unable to copy file {} to folder {}".format(fi, content_target_final_dir), _ERROR)
            else:
                # Vizualization extensions files exist
                debug("Vizualization extension file {} exists".format(fi), _INFO)
                if visualization_extn_dir != "None":
                    debug("Vizualization extension directory is defined: {}".format(visualization_extn_dir), _DEBUG)

                    content_target_final_dir = visualization_extn_dir
                    debug("Copying Visualization extension file {} into folder {}".format(fi, content_target_final_dir), _INFO)

                    try:
                        shutil.copy2(fi, content_target_final_dir)
                    except IOError:
                        debug("Unable to copy file {} to folder {}".format(fi, content_target_final_dir), _ERROR)

                else:
                    debug("Vizualization extension directory is not defined", _WARNING)
                    debug("Visualization extension file {} will not be copied".format(fi), _WARNING)

    # We need to compare provided PS content with that of deployed.
    debug("*********************************************************************")
    if ps_files:
        debug("PS content was provided, will perform content check", _INFO)
        debug("Comparing provided PS content vs. deployed PS content", _INFO)
        debug("PS Model Files will be checked", _INFO)
        debug("Application tokens processed: \n\t{}".format('\n\t'.join(app_tokens)), _INFO)
        debug("Checking PS Model files", _INFO)

        ps_models_repo = [f for f in ps_files if re.search('^(c_)\S+model[.]lkml', f)]
        #ps_models_repo = [f for f in ps_files if re.search('^(c_)\S+model[.]lkml', f)]
        debug("PS Repository Model Files list: \n\t{}".format('\n\t'.join(ps_models_repo)), _INFO)

        # Retrieve PS deployed content
        #ps_models = [f for f in ps_files if re.search('^(c_\S*{0})\S+model[.]lkml'.format([tk for tk in app_tokens]), f)]
        ps_models_deployed = [f for f in get_files(os.path.join(CLIENT_PROJECT_DEPLOYMENT_DIR), fpath=False) if re.search('^(c_)\S+model[.]lkml', f)]
        debug("PS Deployed Model Files list: \n\t{}".format('\n\t'.join(ps_models_deployed)), _INFO)

        # Compare Provided and Deployed Model files and compile a report
        ps_models_not_deployed = set(ps_models_repo).difference(ps_models_deployed)
        if ps_models_not_deployed:
            debug("Some PS Model files were not deployed", _WARNING)
            debug("Not deployed PS Model files: \n\t{}".format('\n\t'.join(ps_models_not_deployed)), _WARNING)
            debug("Possible reasons: \n\
                  1. PS Model file name is missing application token.\n\
                  2. PS Model File name does not comply with Model File naming convention: \n\
                      \tc_<zero or more characters><APPLICATION_TOKEN><zero or more characters>.model.lkml", _WARNING)
            debug("Check PS Model file names", _WARNING)
        else:
            debug("All PS Model Files were deployed", _INFO)
            debug("***** Looker offline project {} deployment completed successfully".format(looker_project_name),
                  _INFO)

        debug("*********************************************************************")
    else:
        # Generate deployment summary.
        if deployment_summary(client_properties, CLIENT_PROJECT_DEPLOYMENT_DIR):
            debug("***** Looker offline project {} deployment completed successfully".format(looker_project_name), _INFO)
        else:
            debug("***** Not all content was deployed. Check your deployment", _WARNING)

def deployment_summary(client_properties, client_project_deployment_dir):
    """
    :param client_properties:
    :param client_deployment_dir:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Provide deployment summary
    # Product name
    # Application names
    # Models (count) deployed for each application
    # Dashboards (count) deployed for each application
    # Views (count) deployed for each applicaiton

    expected_content_elements = ['dashboard', 'view', 'model', 'readme']

    product_installed = client_properties["product_prefix"]

    apps_installed = client_properties["product_apps"]
    # Get content
    deployed_content = get_files(client_project_deployment_dir, fpath=False)
    debug("***** Deployment summary", _INFO)
    debug("     Product installed: {}".format(product_installed))
    debug("     Applications installed: {}".format(apps_installed))

    summary = {}
    for fi in deployed_content:
        if re.search('([.]md)', fi):
            summary["readme"] = summary.get("readme", 0) + 1
        if re.search('(model[.]lkml)', fi):
            summary["model"] = summary.get("model", 0) + 1
        if re.search('(dashboard[.]lookml)', fi):
            summary["dashboard"] = summary.get("dashboard", 0) + 1
        if re.search('(view[.]lkml)', fi):
            summary["view"] = summary.get("view", 0) + 1

    deployed_content_elements = list(summary.keys())
    debug("Looker content elements are: {}".format(deployed_content_elements), _DEBUG)
    found_content_elements = set(expected_content_elements).intersection(deployed_content_elements)
    if set(expected_content_elements) == found_content_elements:
        debug(" Document files deployed: {}".format(summary["readme"]), _INFO)
        debug(" Model files deployed: {}".format(summary["model"]), _INFO)
        debug(" Dashboard files deployed: {}".format(summary["dashboard"]), _INFO)
        debug(" View files deployed: {}".format(summary["view"]), _INFO)
        return True
    else:
        missing_content_elements = set(expected_content_elements).difference(found_content_elements)
        debug("This content is missing: {}".format(missing_content_elements), _WARNING)
        return False

def get_files(directory, *fnpattern, fpath = False):
    """
    Args:
        directory - starting directory to list files
        fpath - flag indicates whether to return file path and name or just name
            False - returns only list of filenames
            True  - returns fully qualified list of filenames: path/filename
        Function generates the file names in a directory
        tree by walking the tree either top-down or bottom-up. For each
        directory in the tree rooted at directory top (including top itself),
        it yields a 3-tuple (dirpath, dirnames, filenames).
        fnpattern - list of file name patterns. If defined, returns list of
            files whose names match pattern specified
    """
    file_paths = []  # List stores filenames with or without path.
    #file_paths_pattern = []

    # Walk directory tree.
    #for root, directories, files in os.listdir(directory):
    for root, directories, files in os.walk(directory, topdown=True):
        directories.clear() # in combination with topdown=True prevents all dir traversing.
        for filename in files:
            if fpath:
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)
            else:
                file_paths.append(filename)

    if fnpattern:
        fnpattern_list = list(fnpattern)
        file_paths_pattern = list()
        for fnp in fnpattern_list[0]:
            debug("Matching file name pattern: {}".format(fnp), _DEBUG)
            match_prefix = re.compile('.*({0}).*'.format(fnp))
            [file_paths_pattern.append(fn) for fn in file_paths if match_prefix.match(fn)]

        file_paths.clear()
        file_paths = list(file_paths_pattern)

    return file_paths

def access_cofiguration(access_config_file, client_properties, in_access_token, ClientID):
    """
    :param access_config_file:
    :param client_properties:
    :param in_access_token:
    :param ClientID:
    :return:
    """
    debug("**********************************************************************")
    debug("*** Function call - {} ***".format(sys._getframe().f_code.co_name), _INFO)
    debug("**********************************************************************")

    product_deployed = client_properties["product_prefix"]

    debug(" Reading access configuration properties for Customer: {} and Product: {}".format(ClientID, product_deployed), _INFO)
    access_config_prop = get_json_prop(access_config_file)["group_role_map"]
    debug(" User requested to confgure these groups/roles: {}".format(access_config_prop), _INFO)

    # Get all roles for product
    roles_expected = client_properties[product_deployed]["roles"]
    debug(" Expected Roles name tokens: {}".format(roles_expected), _INFO)

    current_roles = looker_get_roles(client_properties, in_access_token, ClientID, role_details=True)
    debug(" Roles:'\n' {} for Product: {} exist on Looker instance".format(current_roles, ClientID), _INFO)

    if current_roles:
        user_groups_to_create = list()
        user_groups_access_config = access_config_prop.keys()
        for group_name in user_groups_access_config:
            debug("Checking User defined Group {} for replacement token".format(group_name))
            match_group = re.match('^(.*)(@CLIENT_ID@)(.*)$', group_name)
            if match_group is not None:
                #debug("User defined Group: {} contains token for replacement with Client_ID".format(group_name))
                new_group_name = group_name.replace("@CLIENT_ID@", ClientID.upper())
                user_groups_to_create.append(new_group_name)
            else:
                user_groups_to_create.append(group_name)

        debug("Group Names to be created:'\n'{}".format(user_groups_to_create), _DEBUG)
        looker_create_group(client_properties, in_access_token, ClientID, user_groups_to_create)
        debug("Getting just created Groups IDs:", _INFO)
        user_defined_groups = looker_get_groups(client_properties, in_access_token, ClientID, user_groups_to_create)
        debug("Groups created by User input:'\n'{}".format(user_defined_groups), _DEBUG)
    else:
        debug("Roles for access configuration do not exist", _ERROR)
        debug("Make sure Roles based on Permission Set/Model exist", _ERROR)
        debug("Possible cause - project deployment was not run", _ERROR)

    for role_name, role_attr in current_roles.items():
        # Initialize empty list to keep all groups for a given role
        group_id_list = list()
        role_permission_set = role_attr["permission_set"]["name"]
        role_id = role_attr["id"]
        role_model = role_attr["model_set"]["models"]
        #uncomment for debugging
        debug("")
        debug("")
        debug("--------------------------------------------------------------------------------")
        debug("Begin matching and update sequence", _INFO)
        debug(" *** Processing Role: {} with permission set: {} and model: {}".format(role_name, role_permission_set, role_model), _INFO)
        debug("--------------------------------------------------------------------------------")

        for group_name, group_attr in access_config_prop.items():
            # Need to check if user requested appending Client_ID
            match_group = re.match('^(.*)(@CLIENT_ID@)(.*)$', group_name)
            if match_group is not None:
                #debug("User defined Group: {} contains token for replacement with Client_ID".format(group_name), _INFO)
                new_group_name = group_name.replace("@CLIENT_ID@", ClientID.upper())
                group_name = new_group_name

            group_id = user_defined_groups[group_name]

            debug(" Group-Role assignment properties: Group Name/ID: {}/{} - Group Attr: {}".format(group_name, group_id, group_attr))
            debug("--------------------------------------------------------------------------------")

            for group_attr_key, group_attr_value in group_attr.items():
                group_permission_set = group_attr_key
                expected_group_models = [modelIter + '_' + ClientID for modelIter in group_attr_value]
                #debug(" *** Group name: {} with permission set: {} and model: {}".format(group_name, group_permission_set, expected_group_models), _DEBUG)
                debug(" Matching Role Permission Set: {} and Group Permission Set defined by user: {}".format(role_permission_set, group_permission_set), _DEBUG)
                if role_permission_set == group_permission_set:
                    #debug("  Role and Group Permission sets match. Matching models", _DEBUG)
                    debug("  Matching Role Model: {} and Group Models: {}".format(role_model, expected_group_models), _DEBUG)
                    debug("")
                    debug("")
                    if set(role_model).intersection(set(expected_group_models)):
                        debug("   Role and Group Models match.", _DEBUG)
                        #debug("   Updating Role Name/ID {}/{} with Group Name/ID {}/{}".format(role_name, role_id, group_name, group_id), _DEBUG)
                        debug("")
                        debug("")
                        group_id_list.append(group_id)
                        #looker_update_role_groups_user(client_properties, in_access_token, ClientID, role_id, role_name, group_id, group_name)
        if group_id_list:
            debug("Group IDs List: {} Role ID {}".format(group_id_list, role_id), _DEBUG)
            looker_update_role_groups_user(client_properties, in_access_token, ClientID, role_id, role_name,
                                           group_id_list, group_name)
        else:
            debug("There is no Group for Role Name/ID {}/{}".format(role_name, role_id))

def match_replace_token(client_properties, replacement_tokens_prop, client_project_deployment_dir):
    """
    :param in_string:
    :param in_pattern:
    :return: True/False
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    debug("Performing token replacement", _INFO)
    product_deployed = client_properties["product_prefix"]
    token_indicator = client_properties["token_indicator"]

    replacement_token_map = replacement_tokens_prop[product_deployed].get("replace_token_map")
    debug("Replacement tokens and values:\n{}".format(replacement_token_map))

    file_name_pattern = ["model.lkml", "dashboard.lookml"]
    process_files = get_files(client_project_deployment_dir, file_name_pattern, fpath=True)

    if replacement_token_map is not None:
        #debug("User requested tokens replacement", _INFO)
        for replace_token, replace_value in replacement_token_map.items():
            replace_token = token_indicator+replace_token+token_indicator
            debug(" Replace Token: {} with Value: {}".format(replace_token, replace_value), _INFO)

            match_token = re.compile(r'(.*)({0})(.*)'.format(replace_token))

            with fileinput.input(files=(process_files), inplace=True) as rfh1:
                for line in rfh1:
                    print(match_token.sub(r'\1{0}\3'.format(replace_value), line), end='')
    else:
        debug("No user input. Replacing tokens with default values", _INFO)


def generate_build_manifest(client_properties):
    """
    :param client_properties:
    :return: creates build manifest file
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    # Create and open build manifest file for writing
    build_manifest_file_name = "modn_looker_build{}.properties".format(get_date_timestamp())
    build_manifest_file = os.path.join(CONTENT_TARGET_DIR, build_manifest_file_name)
    # Collect repositories information
    prod_repo = client_properties["prod_repo"]
    prod_branch = client_properties["prod_repo_branch"]
    if not prod_branch:
        prod_branch = "master"

    ps_repo = client_properties["ps_repo"]
    ps_branch = client_properties["ps_repo_branch"]
    if ps_repo:
        if not ps_branch:
            ps_branch = "master"

    # Collect model files installed. It is possible that multiple products are isntalled
    # into the same project so getting models via API is unrilable -
    # other models could exist on the server or the same models already configured.
    # Will get list of model files ready to be copied into target project directory.
    # this seems the most reliable.
    ["model.lkml", "dashboard.lookml"]
    model_files = get_files(CLIENT_PROJECT_DEPLOYMENT_DIR, ["model[.]lkml"])

    with open(build_manifest_file, 'a') as build_manifest:
        build_manifest.write("Build started at: {}\n".format(build_start_dt))
        build_manifest.write("Project name: {}\n".format(LOOKER_PROJECT_NAME))
        build_manifest.write("Production repository: {}\n".format(prod_repo))
        build_manifest.write("Production repository branch: {}\n".format(prod_branch))
        if ps_repo:
            build_manifest.write("PS repository: {}\n".format(ps_repo))
            build_manifest.write("PS repository branch: {}\n".format(ps_branch))
        build_manifest.write("Models deployed: \n")
        for mi in model_files:
            build_manifest.write("  "+mi+'\n')
        build_manifest.write("Build completed at: {}\n".format(get_date_timestamp(current_time=True)))




def cleanup(client_properties, in_access_token):
    """
    Function performs the following:
    1. Deletes OOB db connection
    2. Deletes OOB Model Sets
    3. Deletes OOB Permission Set
    4. ...

    :param client_properties:
    :param in_access_token:
    :return:
    """

# class GetFunctionName():
#     """
#     Class returns function name being called
#     """
#     @staticmethod
#     def name():
#         FuncName =   format(sys._getframe().f_code.co_name)
#         return FuncName

# ***** Main function ***** #
def main():

    debug("Processing looker properties file", _INFO)

    requests.packages.urllib3.disable_warnings()

    # Setup parsing command line arguments and app usage help
    parseArgs = argparse.ArgumentParser(description='Provide the following parameters:')
    parseArgs.add_argument('Client_ID', metavar='CLIENT_ID', type=str,
                           help='Please provide unique Client ID. Mandatory')
    parseArgs.add_argument('-prop_file', type=argparse.FileType('r', encoding='UTF-8'),
                           help='Please provide json formatted properties file name', required=True,
                           default='looker_deployment.json')

    parseArgs.add_argument('-internal_prop_file', type=argparse.FileType('r', encoding='UTF-8'),
                           help='Please provide json formatted properties file name', required=False,
                           default='internal_looker_properties.json')

    # parseArgs.add_argument('-oob_replacement_tokens', type=argparse.FileType('r', encoding='UTF-8'),
    #                        help='Please provide json formatted properties file name', required=False,
    #                        default='replacement_tokens.json')

    parseArgs.add_argument('-upgrade', type=str,
                           help='If = Y - upgrades Customer repository.',
                           default='N', choices=['Y', 'N'])

    parseArgs.add_argument('-cleanup', type=str, help='If = Y - Cleanup environment',
                           default='N', choices=['Y', 'N'])
    parseArgs.add_argument('-deployment_flag', type=str,
                           help='Performs install and configuration or only post-install configuration',
                           required=False,
                           default='install', choices=['install', 'update_user_attributes', 'access_config'])

    args = parseArgs.parse_args()

    # Define service variables to control execution flow
    deployment_flag = args.deployment_flag

    # Make sure ClientID is a valid string, not path and force it to lower case for consistency
    ClientID = args.Client_ID.lower()
    debug("ClientID type: {}".format(type(ClientID)), _DEBUG)
    # Enforce ClientID naming convention:
    # 1. It cannot contain special characters and slashes
    # 2. It cannot be longer than 30 characters
    if re.search(r'[/@#%^\\\-]+', ClientID) or (len(ClientID) > 30):
        debug("ClientID contains invalid characters or too long. Please enter valid ClientID", _ERROR)
        debug("Deployment will be aborted")
        exit(1)


    debug("Deployment for Customer {} started".format(ClientID), _INFO)
    debug("Arguments passed: {}".format(vars(parseArgs.parse_args())), _INFO)

# Check for correct deployment flag combinations
    if (args.upgrade == 'Y') and (args.cleanup == 'Y'):
        debug("Cannot run Cleanup and Upgrade at the same time. Aborting deployment", _ERROR)
        exit(1)

# Read internal properties
    debug("Reading Looker internal properties", _INFO)
    _internal_prop = get_json_prop(args.internal_prop_file)

 # Read Client properties required for deployment
    debug("Reading client properties file.", _INFO)
    _client_prop = get_json_prop(args.prop_file)

    debug("Constructing combined properties dictionary for application consumption", _INFO)
    client_prop = {**_internal_prop, **_client_prop}

    # debug("Reading replacement tokens properties", _INFO)
    # _oob_replacement_tokens = get_json_prop(args.oob_replacement_tokens)

    global _DEBUG_LEVEL
    _DEBUG_LEVEL = _DEBUG_CONF_LEVEL.get(client_prop.get("debug_level", "X"), _DEBUG_LEVEL)

    debug("Setting up deployment environment folder structure", _INFO)
    # Setup Client folder and log folder under it for deployment and configuration

    os.environ['MN_LOOKER_DEPLOYMENT_BASE'] = os.path.expanduser(client_prop["looker_deployment_base"])

    if os.environ.get('MN_LOOKER_DEPLOYMENT_BASE') is None:
        debug("MN_LOOKER_DEPLOYMENT_BASE is not defined. Set it in properties file and try again. Exiting...", _ERROR)
        exit(1)
    else:
        _DEPLOYMENT_BASE = os.environ['MN_LOOKER_DEPLOYMENT_BASE']
        debug("Looker Deployment Base directory: {}".format(_DEPLOYMENT_BASE), _INFO)
        # Create Client specific deployment directory

        CLIENT_DEPLOYMENT_DIR = os.path.join(_DEPLOYMENT_BASE, ClientID)

        # Define logging directory
        CLIENT_DEPLOYMENT_DIR_LOG = os.path.join(CLIENT_DEPLOYMENT_DIR, client_prop["log_directory"])

        # Check if Customer directory exists
        if not os.path.isdir(CLIENT_DEPLOYMENT_DIR):
            debug("Client Deployment directory {} does not exist and will be created".format(CLIENT_DEPLOYMENT_DIR), _INFO)
            os.mkdir(CLIENT_DEPLOYMENT_DIR)
        else:
            debug("Client Deployment directory {} exists, skipping creation".format(CLIENT_DEPLOYMENT_DIR), _INFO)

        # Check if log directiry exists under Customer folder
        if not os.path.isdir(CLIENT_DEPLOYMENT_DIR_LOG):
            debug("Client Log directory {} does not exist and will be created".format(CLIENT_DEPLOYMENT_DIR_LOG))
            os.mkdir(CLIENT_DEPLOYMENT_DIR_LOG)
        else:
            debug("Client Log directory {} exists".format(CLIENT_DEPLOYMENT_DIR_LOG), _INFO)

    # Enable logging
    log_file_short_name = "looker_installer{}.log".format(get_date_timestamp())
    log_file_name = os.path.join(CLIENT_DEPLOYMENT_DIR_LOG, log_file_short_name)

    logging.basicConfig(filename=log_file_name, level=logging.INFO)
    global _LOGGER
    _LOGGER = logging.getLogger()
    debug("Looker Deployment Base directory: {}".format(_DEPLOYMENT_BASE), _INFO)
    debug("Client ID: {}".format(ClientID), _INFO)
    debug("Customer Deployment directory: {}".format(CLIENT_DEPLOYMENT_DIR), _INFO)
    debug("Continue deployment from Customer directory: {}".format(CLIENT_DEPLOYMENT_DIR), _INFO)
    os.chdir(CLIENT_DEPLOYMENT_DIR)

    global build_start_dt
    build_start_dt = get_date_timestamp(current_time=True)
    # debug("********** Started Looker Environment deployment at {} **********".format(get_date_timestamp(current_time=True)))
    debug("********** Started Looker Environment deployment at {} **********".format(build_start_dt))


# Start deployment

    # Check defined properties for consistency
    check_prod_apps_models(client_prop)

    debug("Getting Looker access token", _INFO)
    current_access_token = get_access_token(client_prop)

# Start of Deployment phase
    if deployment_flag == 'install':
    # We need to create db connectons before creating models so models will be valid right after deployment
        # Create connection
        # "Creating new db connection"
        db_conn_name = looker_create_dbconnection(client_prop, current_access_token, ClientID)
        debug("Checking just created connection: {}".format(db_conn_name), _DEBUG)

        # Test connection
        debug("Testing created connection", _INFO)
        looker_test_dbconnection(client_prop, current_access_token, db_conn_name)

    # Code base deployment. This is complete deployment (project is conected to customer github repo)
        if client_prop["project_mode"] == 'remote':
            # project must already exist when deploying in this mode
            # Looker Project Configuration
            # INFO - https://discourse.looker.com/t/using-the-api-to-configure-your-projects-git-connection/5433
            debug("Configuring Looker Project - {}".format(client_prop["project_name"]))
            debug("Creating Project Deploy Key", _INFO)
            looker_create_deploy_key(client_prop, current_access_token)
            debug("Updating Project", _INFO)
            looker_update_project(client_prop, current_access_token)
            debug("Project Properties:", _INFO)
            looker_get_project(client_prop, current_access_token)

            if args.upgrade == 'Y':
                # Update portion
                    # Full clone Production repository
                    update_customer_repository(client_prop)
                    prod_files = get_files(os.path.join(CLIENT_DEPLOYMENT_DIR, client_prop["prod_repo"]), fpath=True)

                    # Copy content of Production repo into Customer repo
                    for iter_file in prod_files:
                        shutil.copy(iter_file, os.path.join(CLIENT_DEPLOYMENT_DIR, client_prop["customer_repo"]))

                    # Push synced up content from cloned local Customer repo into Customer GitHub repo
                    os.chdir(client_prop["customer_repo"])
                    git_add = "git add *"
                    #debug(git_add.split())

                    git_push = "git push git@github.com:ModelN/"+client_prop["customer_repo"]+".git"
                    debug("Push command: {}".format(git_push.split()))

                    debug("Running git add", _INFO)
                    subprocess.run(git_add.split(), timeout=60)
                    debug("Running git commit")
                    subprocess.run(["git", "commit", "-m", "Syncup Customer GitHub"], timeout=60)
                    debug("Running git push", _INFO)
                    subprocess.run(git_push.split(), timeout=60)
                # End of update portion
            else:
                debug("Executing Customer GitHub first time repository initialization", _INFO)
                initiate_customer_repository(client_prop, CLIENT_DEPLOYMENT_DIR)

        else:
            if client_prop["project_mode"] == 'offline':
                offline_deployment(client_prop, CLIENT_DEPLOYMENT_DIR, ClientID, db_conn_name)

    # Clean up sequence. It deletes Models, db connection, Modle Sets, User Roles
        if args.cleanup == 'Y':
            # 1. Delete Models
            debug("Deleting Models", _INFO)
            # 2. Delete Roles
            debug("Removing Model Sets", _INFO)
            looker_delete_model_set(client_prop, current_access_token)


        # Configure LookML Models
        debug("Configure LookML Models", _INFO)
        looker_create_lookml_model(client_prop, CLIENT_PROJECT_DEPLOYMENT_DIR, current_access_token, db_conn_name, LOOKER_PROJECT_NAME)

    # TO DO - add model checking for has_content attribute

        # *** Configure Data Access ***
        product_deployed = client_prop["product_prefix"]
        create_roles = client_prop[product_deployed]["roles"]
        if create_roles:
            debug("Product {} requires Roles {} configuration".format(product_deployed.upper(), create_roles), _INFO)
            debug("The following steps will be performed:", _INFO)
            debug("     1. Create Model Sets", _INFO)
            debug("     2. Create Permission Sets", _INFO)
            debug("     3. Create Roles based on Model and Permission Sets", _INFO)
            debug("**********************************************************************")
            # Get LookML models
            debug("Getting LookML Models configured on Looker instance for Product", _INFO)
            current_lookml_models = looker_get_lookml_models(client_prop, current_access_token, ClientID)

            # Create Model Sets - one per available model per Client
            debug("Creating Model Sets", _INFO)
            looker_create_model_set(client_prop, current_access_token, current_lookml_models, ClientID)

            # Getting existing Model Sets
            debug("Getting Model Sets", _INFO)
            current_model_sets = looker_get_model_sets(client_prop, current_access_token, current_lookml_models, ClientID)

            # Create Permission Sets (App User and App Power User)
            debug("Creating Permission Sets", _INFO)
            looker_create_permission_set(client_prop, current_access_token)

            # Get All Permission Sets
            debug("Getting Permission Sets", _INFO)
            current_permission_sets = looker_get_permission_sets(client_prop, current_access_token)

            # Create Roles per Application (Model Set/Permission Set)
            debug("Creating Roles", _INFO)
            looker_create_role(client_prop, current_access_token, current_lookml_models, current_model_sets, current_permission_sets, ClientID)

            groups_config = client_prop.get("groups", "None")
            if groups_config != 'None':
                create_groups = client_prop[product_deployed]["groups"]
                if create_groups:
                #if product_deployed == 'cdm':
                    debug("Product {} requires Groups creation and Role-Group assignment".format(product_deployed.upper()), _INFO)

                    debug(" Creating Groups", _INFO)
                    looker_create_group(client_prop, current_access_token, ClientID)

                    debug("Getting Groups", _INFO)
                    current_groups = looker_get_groups(client_prop, current_access_token, ClientID)

                    debug("Getting Roles for Group assignment", _INFO)
                    current_roles = looker_get_roles(client_prop, current_access_token, ClientID)

                    #debug("     Getting Groups for Role", _INFO)
                    #looker_get_role_groups(client_prop, current_access_token, 99)

                    debug("Assigning Groups to Roles", _INFO)
                    looker_update_role_groups(client_prop, current_access_token, ClientID, current_roles, current_groups)
                else:
                    debug("Groups are not configured for Product {}".format(product_deployed.upper()), _INFO)
            else:
                debug("Product {} does not require Group configuration".format(product_deployed.upper()), _INFO)
        # *** End of Configure Data Access ***



        debug("Creating User Attributes", _INFO)
        looker_create_user_attribute(client_prop, current_access_token, ClientID)

    # End of Deployment phase

    # Setup webhook for github repo not connected to Looker project.
    # https://discourse.looker.com/t/looker-project-git-pull-endpoint/3651/2

    if (deployment_flag == 'update_user_attributes' or deployment_flag == 'install'):
        debug("Performing post-install updates", _INFO)
        debug(" Updating User Attributes", _INFO)
        user_attr = looker_get_user_attributes(client_prop, current_access_token, ClientID)
        debug("Server User Attributes: {}".format(user_attr))
        # Update user attributes
        looker_update_user_attribute(client_prop, current_access_token, user_attr)


    # Access configuration sequence
    # Check if access config file is present in looker_deployment_base folder
    access_config_file_name = client_prop["access_config_file_name"]
    access_config_file_exist = os.path.isfile(os.path.join(_DEPLOYMENT_BASE, access_config_file_name))
    if access_config_file_exist:
        _access_config_file = os.path.join(_DEPLOYMENT_BASE, 'access_config.json')
        debug("File {} exists, application will create access configuration".format(_access_config_file), _INFO)
        try:
            access_config_file = open(_access_config_file, encoding='UTF-8')
            access_cofiguration(access_config_file, client_prop, current_access_token, ClientID)
        except IOError:
            debug("Cannot read file {}".format(_access_config_file), _ERROR)

    debug("Generating build manifest file", _INFO)
    generate_build_manifest(client_prop)

    # Logout after all done
    debug("Logging out", _INFO)
    looker_logout(client_prop, current_access_token)

# Main execution.
if __name__ == '__main__':
    main()
