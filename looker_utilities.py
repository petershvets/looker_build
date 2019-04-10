import argparse
import json
from collections import OrderedDict
import requests
import datetime
import subprocess
import os
import shutil
import re
from distutils.dir_util import copy_tree
import logging
import fileinput
import sys
import looker_deployment
from looker_deployment import get_json_prop
from looker_deployment import get_date_timestamp
from looker_deployment import get_access_token
from looker_deployment import run_looker_restapi
from looker_deployment import get_response_code
from collections import defaultdict

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


_FILTER_DEFAULT_VALUES = {}
_GLOBAL_SUMMARY = []
_QUERY_KEYS_TO_REMOVE = ["client_id", "share_url", "expanded_share_url", "url"]
_LOOK_KEYS_TO_REMOVE = ["url","short_url","content_metadata_id","id","last_updater_id","created_at","space","content_favorite_id"]
_LOOK_EXT = "look.json"
_DASHBOARD_EXT = "dash.json"

def debug(msg, level=_MESSAGE, json_flag=False):
    looker_deployment.debug(msg, level, json_flag)

def save_json_file(data_dir, filename, file_data):
    """
    API to save json file to the file system
    :param data_dir:
    :param filename:
    :param file_data:
    """
    with open(os.path.join(data_dir, filename), 'w') as outfile:
        json.dump(file_data, outfile, indent=3, sort_keys=True)

def copy_dict(attr_list, source_dict, target_dict = {}):
    """
    API to copy specific values from a dictionary
    :param attr_list:
    :param source_dict:
    :param target_dict:
    """
    for attr in attr_list:
        target_dict[attr] = source_dict.get(attr, None)

    return target_dict

def replace_model(client_properties, model_name):
    """
    :param client_properties:
    :param model_name:
    :return new_model_name:
    """

    # if model name not defined, just return null back
    if not model_name:
        return model_name
    new_model_name = model_name
    model_remap = client_properties.get("model_remap",{})
    for (remap_key, remap_val) in model_remap.items():
        # We can have 3 cases
        # "":"something" In this case we just append suffix
        # "something":"" In this case we just remove suffix
        # "something":""something In this case we just replace
        if remap_key == "" and remap_val != "":
            new_model_name = "{}_{}".format(model_name, remap_val)
            break
        elif remap_key != "" and remap_val == "" and model_name.endswith("_" + remap_key):
            # Remove client at the end
            new_model_name = model_name[:-len(remap_key)-1]
            break
        elif remap_key != "" and remap_val != "" and model_name.endswith("_" + remap_key):
            # Remove client at the end
            new_model_name = "{}{}".format(model_name[:-len(remap_key)], remap_val)
            break

    if new_model_name != model_name:
        debug("Remapping model name from {} to {}".format(model_name, new_model_name), _INFO)

    return new_model_name


def get_namespace(client_properties, namespace, space_list):
    """
    :param client_properties:
    :param namespace:
    :param space_list:
    :return:
    """
    new_namespace = namespace
    if not client_properties["space_remap"]:
        debug("Mandatory space_remap not provided.", _WARNING)
        return None, None

    # Find namespaces to import to.
    # 1. "Old Namespace":"New Namespace" - we use the map
    # 2. "":"Something" - everything will be mapped to New Namespace
    # 3. "Old Namespace":"" - everything for old will be mapped to original
    # 4. "":"" - everything will be mapped to original
    # If we cannot figure out, we will skip the object

    if namespace in client_properties["space_remap"].keys():
        # Case 1 or 3 - namespace mapping defines from-to
        if client_properties["space_remap"][namespace]:
            # Case 1 - find new name
            new_namespace = client_properties["space_remap"][namespace]
        # Case 3 - just use original name
    elif "" in client_properties["space_remap"].keys():
        # Case 2 or 4
        if client_properties["space_remap"][""] != "":
            # Case 2 - everything goes to new namespace
            new_namespace = client_properties["space_remap"][""]
            debug("Using default remap space {}".format(new_namespace), _INFO)
        else:
            # Case 4 - everything goes to original namespace
            debug("Using default empty remap space {}".format(new_namespace), _INFO)
    else:
        # No mapping provided fo rthe namespace. We will retunr not found and skip import
        debug("Cannot find target namespace from {}. Check remap_namespace.".format(namespace), _WARNING)
        return None, None

    # Looking for a space to remap
    if new_namespace in space_list.keys():
        namespace_id = space_list[new_namespace]
    else:
        debug("Cannot find target namespace {}".format(new_namespace), _WARNING)
        return None, None

    return namespace_id, new_namespace


def looker_look(client_properties, in_access_token, look_id):
    """
    :param client_properties:
    :param in_access_token:
    :param look_id:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_LOOK", look_id)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        return body
    else:
        debug("Cannot get Look: {}".format(body["message"]), _WARNING)



def looker_looks(client_properties, in_access_token, all_looks = False):
    """
    :param client_properties:
    :param in_access_token:
    :param space_name:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    namespaces = client_properties["space_remap"]

    r = run_looker_restapi(client_properties, in_access_token, "GET_LOOKS")
    resp_code = get_response_code(r)
    body = r.json()
    looks = []

    if resp_code == 200:
        if all_looks:
            # we just return all looks if requested w/o body
            debug("Return ALL non-deleted server looks", _INFO)
            return [el for el in body if not el["deleted"]]
        for look in body:
            if len(namespaces) == 0 or "" in namespaces.keys() or look["space"]["name"] in namespaces.keys():
                if look["deleted"]:
                    debug("Look deleted, skipping: {}: {} ".format(look["space"]["name"], look["title"]), _INFO)
                else:
                    debug("Found Look {}: {}".format(look["space"]["name"], look["title"]), _INFO)
                    look["look_json"] = looker_look(client_properties, in_access_token, look["id"])
                    looks.append(look)
        return looks
    else:
        debug("Cannot get Looks: {}".format(body["message"]), _WARNING)

def looker_spaces(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :param space_name:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_SPACES")
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
       return  {el["name"]:el["id"] for el in body}
    else:
        debug("Cannot get Spaces: {}".format(body["message"]), _WARNING)


def looker_get_dashboard(client_properties, in_access_token, dashboard_id):
    """
    :param client_properties:
    :param in_access_token:
    :param dashboard_id:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_DASHBOARD", dashboard_id)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        return body
    else:
        debug("Cannot get Dashboard: {}".format(body["message"]), _WARNING)


def looker_create_query(client_properties, in_access_token, query):
    """
    :param client_properties:
    :param in_access_token:
    :param query:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    query_id = query["id"]
    new_query = query.copy()
    for key in _QUERY_KEYS_TO_REMOVE:
        del new_query[key]
    #print(json.dumps(new_query, indent=4, separators=(',', ': ')))
    new_query["model"] = replace_model(client_properties, new_query["model"])

    r = run_looker_restapi(client_properties, in_access_token, "CREATE_QUERY", in_payload=new_query)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        debug("Created Query (original id: {}) : {}".format(query_id, body["id"]), _INFO)
        return body["id"]
    else:
        debug("Cannot create Query {}".format(body["message"]), _WARNING)
        return


def looker_create_look(client_properties, in_access_token, look, space_list):
    """
    :param client_properties:
    :param in_access_token:
    :param look:
    :param space_list:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Fetching namespace of the original look

    namespace = look["space"]["name"]
    (namespace_id, new_namespace) = get_namespace(client_properties, namespace, space_list)
    title = "{}{}".format(client_properties.get("name_prefix", ""), look["title"])
    debug("Creating Look {}, space: {} ({}) ".format(title, new_namespace, namespace_id), _INFO)
    if not namespace_id:
        debug("Cannot find space for Look {}, space: {} skipping creation. ".format(title, namespace), _WARNING)
        return

    query_id = looker_create_query(client_properties, in_access_token, look["query"])
    if not query_id:
        return

    new_look = {
        "query_id": query_id,
        "space_id": namespace_id,
        "title": title
    }

    copy_dict(["can", "description", "is_run_on_load"], look, new_look)

    r = run_looker_restapi(client_properties, in_access_token, "CREATE_LOOK", in_payload=new_look)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        debug("Created Look (original id: {}) : {}".format(look["id"], body["id"]), _INFO)
        return body["id"]
    else:
        debug("Cannot create look {}".format(body["message"]), _WARNING)
        return


def looker_create_dashboard(client_properties, in_access_token, server_looks, dashboard, space_list):
    """
    :param client_properties:
    :param in_access_token:
    :param server_looks:
    :param dashboard:
    :param space_list:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    # Fetching namespace of the original dashboard
    namespace = dashboard["space"]["name"]
    (namespace_id, new_namespace) = get_namespace(client_properties, namespace, space_list)
    title = "{}{}".format(client_properties.get("name_prefix", ""), dashboard["title"])
    debug("Creating Dashboard {}, space: {} ({}) ".format(title, new_namespace, namespace_id), _INFO)

    if not namespace_id:
        debug("Cannot find space for Dashboard {}, space: {} skipping creation. ".format(title, namespace), _WARNING)
        return


    dashboard_id = None
    error_flag = False
    new_dashboard = {
        "space_id": namespace_id,
        "title": title
    }
    attr_list_to_copy =["hidden",
                        "refresh_interval",
                        "load_configuration",
                        "background_color",
                        "show_title",
                        "title_color",
                        "show_filters_bar",
                        "tile_background_color",
                        "text_tile_text_color",
                        "query_timezone",
                        "can"]

    copy_dict(attr_list_to_copy, dashboard, new_dashboard)

    r = run_looker_restapi(client_properties, in_access_token, "CREATE_DASHBOARD", in_payload=new_dashboard)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        debug("Created Dashboard (original id: {}) : {}".format(dashboard["id"], body["id"]), _INFO)
        dashboard_id = body["id"]
        new_layouts = body["dashboard_layouts"]
    else:
        debug("Cannot create Dashboard {}".format(body["message"]), _WARNING)
        error_flag = True

    if "dashboard_filters" in dashboard and not error_flag:
        if not looker_create_dashboard_filters(client_properties, in_access_token, dashboard_id, dashboard["dashboard_filters"]):
            debug("Cannot Create Dashboard Filters", _WARNING)
            error_flag = True

    if "dashboard_elements" in dashboard and not error_flag:
        element_list = looker_create_dashboard_elements(client_properties, in_access_token, server_looks, dashboard_id,
                                                        namespace_id, dashboard["dashboard_elements"])
        if not element_list:
            debug("Cannot Create Dashboard Elements", _WARNING)
            error_flag = True

    if "dashboard_layouts" in dashboard and not error_flag:
        if not looker_create_dashboard_layouts(client_properties, in_access_token, dashboard_id,
                                               element_list, new_layouts, dashboard["dashboard_layouts"]):
            debug("Cannot Create Dashboard Layouts", _WARNING)
            error_flag = True

    # Check if the flow completed. If not, try to cleanup
    if error_flag and dashboard_id:
        looker_delete_dashboard(client_properties, in_access_token, dashboard_id)
        return False

    return dashboard_id

def looker_create_dashboard_elements(client_properties, in_access_token, server_looks, dashboard_id, namespace_id, element_list):
    """
    :param client_properties:
    :param in_access_token:
    :param dashboard_id:
    :param namespace_id:
    :param element_list:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    new_element_list = {}
    for dashboard_element in element_list:
        debug("Creating element {}".format(dashboard_element["id"]), _INFO)

        new_dashboard_element = dashboard_element.copy()
        if new_dashboard_element["result_maker_id"]:
            del new_dashboard_element["result_maker"]["id"]
        for el in ["id", "look", "result_maker_id"]:
            del new_dashboard_element[el]

        new_dashboard_element["dashboard_id"] = dashboard_id

        if new_dashboard_element["look_id"]:
            # We need to find LOOK_ID
            new_look_id = None
            look_name = dashboard_element["look"]["title"]
            new_look_name = title = "{}{}".format(client_properties.get("name_prefix", ""), look_name)
            # First we try to see if we have a look with remapped name in the new space
            for look in server_looks:
                if look["space_id"] == namespace_id and look["title"] == new_look_name:
                    new_look_id = look["id"]
                    debug("Found look in the new space {} ({}) namespace {}".format(new_look_name, new_look_id, namespace_id), _INFO)
                    break

            if not new_look_id:
                debug("Cannot find look with the provided name {} in the namespace {}".format(new_look_name, namespace_id), _WARNING)
                return {}

            new_dashboard_element["look_id"] = new_look_id

        elif new_dashboard_element["query_id"]:
            debug("Element with query {}, creating...".format(new_dashboard_element["query_id"]), _INFO)
            new_query_id = looker_create_query(client_properties, in_access_token, new_dashboard_element["query"])
            if not new_query_id:
                debug("Cannot create query", _WARNING)
                return {}
            del new_dashboard_element["query"]
            new_dashboard_element["query_id"] = new_query_id

        r = run_looker_restapi(client_properties, in_access_token,
                               "CREATE_DASHBOARD_ELEMENT", in_payload=new_dashboard_element)
        resp_code = get_response_code(r)
        body = r.json()

        if resp_code == 200:
            new_element_id = body["id"]
            debug("Created Dashboard Element {} (original id: {})".format(new_element_id, dashboard_element["id"]), _INFO)
            new_element_list[new_element_id] = dashboard_element["id"]
        else:
            debug("Cannot create element {}".format(body["message"]), _WARNING)
            return {}

    return new_element_list


def looker_create_dashboard_layouts(client_properties, in_access_token, dashboard_id, element_list, new_layouts, layout_list):
    """
    :param client_properties:
    :param in_access_token:
    :param dashboard_id:
    :param element_list:
    :param new_layouts:
    :param layout_list:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    for dashboard_layout in layout_list:
        debug("Creating layout {}".format(dashboard_layout["id"]), _INFO)

        new_layout = dashboard_layout.copy()
        layout_component = dashboard_layout["dashboard_layout_components"]
        del new_layout["id"]
        del new_layout["dashboard_layout_components"]
        new_layout["dashboard_id"] = dashboard_id

        #print(json.dumps(new_layout, indent=4, separators=(',', ': ')))

        r = run_looker_restapi(client_properties, in_access_token, "CREATE_DASHBOARD_LAYOUT", in_payload=new_layout)
        resp_code = get_response_code(r)
        body = r.json()

        if resp_code == 200:
            layout_id = body["id"]
            debug("Created Dashboard Layout {} (original id: {}) ".format(layout_id, dashboard_layout["id"]), _INFO)
            #print(json.dumps(body["dashboard_layout_components"], indent=4, separators=(',', ': ')))
            #Process components
            for component in body["dashboard_layout_components"]:

                old_element_id = element_list [component["dashboard_element_id"]]
                new_component = None
                for old_component in layout_component:
                    if old_component["dashboard_element_id"] ==  old_element_id:
                        new_component = old_component.copy()

                if not new_component:
                    debug("Cannot locate original component {}".format(old_element_id), _WARNING)
                    return False

                new_component["dashboard_layout_id"] = layout_id
                new_component["id"] = component["id"]
                new_component["dashboard_element_id"] = component["dashboard_element_id"]

                #print(json.dumps(new_component, indent=4, separators=(',', ': ')))

                r = run_looker_restapi(client_properties, in_access_token, "UPDATE_DASHBOARD_LAYOUT_COMPONENT",
                                       component["id"], in_payload=new_component)
                resp_code = get_response_code(r)
                body = r.json()

                if resp_code == 200:
                    debug("Updated Dashboard Layout Component {} ".format(new_component["id"]), _INFO)
                else:
                    debug("Cannot update layout component {}".format(body["message"]), _WARNING)
                    return False

        else:
            debug("Cannot create layout {}".format(body["message"]), _WARNING)
            return False
    # Now we delete default layouts created automatically.
    if not looker_delete_dashboard_layouts(client_properties, in_access_token, new_layouts):
        debug("Cannot delete Default Layouts", _WARNING)
        return False

    return True


def looker_delete_dashboard_layouts(client_properties, in_access_token, new_layouts):
    """
    :param client_properties:
    :param in_access_token:
    :param new_layouts:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    for dashboard_layout in new_layouts:
        debug("Deleting old layout {}".format(dashboard_layout["id"]), _INFO)

        r = run_looker_restapi(client_properties, in_access_token, "DELETE_DASHBOARD_LAYOUT", dashboard_layout["id"])
        resp_code = get_response_code(r)
        if resp_code == 204:
            pass
        else:
            body = r.json()
            debug("Cannot delete layout {} {}".format(dashboard_layout["id"], body["message"]), _WARNING)
            return False

    return True

def looker_delete_dashboard(client_properties, in_access_token, dashboard_id):
    """
    :param client_properties:
    :param in_access_token:
    :param dashboard_id:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    debug("Deleting incomplete dashboard {}".format(dashboard_id), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "DELETE_DASHBOARD", dashboard_id)
    resp_code = get_response_code(r)
    if resp_code == 204:
        pass
    else:
        body = r.json()
        debug("Cannot delete dashboard {} {}".format(dashboard_id, body["message"]), _WARNING)
        return False

    return True

def looker_create_dashboard_filters(client_properties, in_access_token, dashboard_id, filter_list):
    """
    :param client_properties:
    :param in_access_token:
    :param dashboard_id:
    :param filter_list:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    for dashboard_filter in filter_list:
        debug("Creating filter {}".format(dashboard_filter["name"]), _INFO)

        new_filter = dashboard_filter.copy()
        del new_filter["id"]
        new_filter["dashboard_id"] = dashboard_id
        new_filter["model"] = replace_model(client_properties, new_filter["model"])

        r = run_looker_restapi(client_properties, in_access_token, "CREATE_DASHBOARD_FILTER", in_payload=new_filter)
        resp_code = get_response_code(r)
        body = r.json()

        if resp_code == 200:
            debug("Created Dashboard Filter {} (original id: {}) : {}".format(new_filter["name"], dashboard_filter["id"], body["id"]), _INFO)
            filter_id = body["id"]
        else:
            debug("Cannot create filter {}".format(body["message"]), _WARNING)
            return False

    return True


def looker_dashboards(client_properties, in_access_token, fetch_dashboards = True):
    """
    :param client_properties:
    :param in_access_token:
    :param fetch_dashboards:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    namespaces = client_properties["space_remap"]

    r = run_looker_restapi(client_properties, in_access_token, "GET_DASHBOARDS")
    resp_code = get_response_code(r)
    body = r.json()
    dashboards = []

    if resp_code == 200:
        if not fetch_dashboards:
            return body
        for dashboard in body:
            if dashboard.get("space", "") and \
                    (len(namespaces) == 0 or "" in namespaces.keys() or dashboard["space"]["name"] in namespaces.keys()):
                debug("Found Dashboard {}: {}".format(dashboard["space"]["name"], dashboard["title"]), _INFO)
                dashboard["dashboard_json"] = looker_get_dashboard(client_properties, in_access_token, dashboard["id"])
                dashboards.append(dashboard)
        return dashboards
    else:
        debug("Cannot get Dashboards: {}".format(body["message"]), _WARNING)

def looker_get_space_id(client_properties, in_access_token, space_name):
    """
    :param client_properties:
    :param in_access_token:
    :param space_name:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "FIND_SPACE", space_name)
    resp_code = get_response_code(r)
    body = r.json()
    space_id = None
    if resp_code == 200:
        if body:
            space_id = body[0]["id"]
            debug("Found space: {} ({})".format(space_name, space_id), _INFO)
        else:
            debug("Space {} not found".format(space_name, space_id), _WARNING)

        return space_id
    else:
        debug("Cannot find space: {}".format(body["message"]), _WARNING)


def looker_who_am_I(client_properties, in_access_token):
    """
    :param client_properties:
    :param in_access_token:
    :return user_id:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "WHO_AM_I")
    resp_code = get_response_code(r)
    body = r.json()
    space_id = None
    if resp_code == 200:
        debug("You are {} {} ({}) user_id: {}".format(body["first_name"], body["last_name"], body["email"], body["id"]), _INFO)
        return body["id"]
    else:
        debug("Cannot determine current user name: {}".format(body["message"]), _WARNING)


def looker_get_explore(client_properties, in_access_token, model_name, exp_name):
    """
    :param client_properties:
    :param in_access_token:
    :param model_name:
    :param exp_name:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_EXPLORE", model_name, exp_name)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        debug("Fetched Explore {}".format(body["id"]), _INFO)
        return body
    else:
        debug("Cannot get explore {}:{} {}".format(model_name, exp_name, body["message"]), _WARNING)
        return

def looker_create_explore_query(client_properties, in_access_token, explore):
    """
    :param client_properties:
    :param in_access_token:
    :param explore:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    debug("Creating query for Explore {} ({} attributes)".format(explore["id"], len(explore["fields"]["dimensions"] + explore["fields"]["measures"])), _INFO)
    global _GLOBAL_SUMMARY
    _GLOBAL_SUMMARY.append("Attribute count: {}".format(len(explore["fields"]["dimensions"] + explore["fields"]["measures"])))
    query_json = {
        "model": explore["model_name"],
        "view": explore["name"],
        "fields": [],
        "filters": {},
        "sorts": [],
        "limit": 20
    }

    for col in (explore["fields"]["dimensions"] + explore["fields"]["measures"])[:client_properties.get("attribute_limit",10000)]:
        if not col["hidden"]:
            query_json["fields"].append(col["name"])
            # Get default value if found.
            if col["name"] in _FILTER_DEFAULT_VALUES.keys():
                # Add a mandatory filter
                debug("Adding generic filter for {}".format(col["name"]), _DEBUG)
                query_json["filters"][col["name"]] =  _FILTER_DEFAULT_VALUES[col["name"]]
            # Check for override at explore level

    if explore["name"] in _FILTER_DEFAULT_VALUES.keys():
        for (flt_name, flt_val) in _FILTER_DEFAULT_VALUES[explore["name"]].items():
            debug("Adding explore filter for {}".format(flt_name), _DEBUG)
            query_json["filters"][flt_name] = flt_val

    if client_properties.get("fast_explore_check","False") == "True":
        # Fast check enabled, adding dummy filter.
        view_name = explore["sql_table_name"].split()
        if view_name:
            view_name = view_name[-1]
        else:
            debug("Cannot find alias {}".format(explore["sql_table_name"]), _WARNING)
        dummy_filter = ""
        for fld in explore["fields"]["dimensions"]:
            if fld["view"] == view_name and fld["type"] == "string":
                dummy_filter = fld["name"]
                break
        if dummy_filter:
            query_json["filters"][dummy_filter] = "101010101"
            debug("Adding DUMMY filter {} for fast check".format(dummy_filter), _DEBUG)
        else:
            debug("Cannot Add DUMMY filter for fast check", _WARNING)

    return query_json


def looker_run_query(client_properties, in_access_token, query, output_format, query_title=""):
    """
    :param client_properties:
    :param in_access_token:
    :param query:
    :param output_format:
    :param query_title:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)
    debug("Running Query {}:{} ({})".format(query["model"], query["view"], query_title), _INFO)
    global _GLOBAL_SUMMARY
    r = run_looker_restapi(client_properties, in_access_token, "RUN_INLINE_QUERY", output_format, in_payload=query)
    resp_code = get_response_code(r)
    body = r.json()

    if resp_code == 200:
        log_msg = "Output {} rows".format(len(body))
        _GLOBAL_SUMMARY.append(log_msg)
        debug(log_msg, _INFO)
        if len(body) >0 and "looker_error" in body[0].keys():
            for looker_err in body:
                err = "SQL ERROR REPORTED {}".format(looker_err["looker_error"])
                _GLOBAL_SUMMARY.append("ERROR: running query: {}".format(err))
                debug(err, _WARNING)
            return False
    else:
       debug("Cannot run query {}".format(body["message"]), _WARNING)
       return False

    return body


def looker_get_lookml_models(client_properties, in_access_token, project_name = ""):
    """
    Function returns
    :param client_properties:
    :param in_access_token:
    :param project_name
    :return: dictionary of models and explores
    """

    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_LOOKML_MODELS")
    resp_code = get_response_code(r)
    body = r.json()

    explore_list = []

    if resp_code == 200:
        for exp in body:
            if project_name == "" or exp["project_name"] == project_name:
                new_model = {
                        "project_name": exp["project_name"],
                        "name": exp["name"],
                        "explores": [nm["name"] for nm in exp["explores"] if client_properties.get("test_hidden_explores","True") == "True" or not nm["hidden"]]
                    }
                explore_list.append(new_model)
                debug("Found model: {}".format(new_model), _INFO)
    else:
        debug("Cannot fetch models/explores {}".format(body["message"]), _WARNING)
        return False

    return explore_list


def looker_cleanup_models(client_properties, in_access_token, project_name = ""):
    """
    Function returns
    :param client_properties:
    :param in_access_token:
    :param project_name
    """

    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    r = run_looker_restapi(client_properties, in_access_token, "GET_LOOKML_MODELS")
    resp_code = get_response_code(r)
    body = r.json()

    explore_list = []

    if resp_code == 200:
        for model in body:
            if not model["has_content"]:
                debug('Deleting model {} '.format(model["name"]), _INFO)
                r = run_looker_restapi(client_properties, in_access_token, "DELETE_LOOKML_MODEL", model["name"])
                resp_code = get_response_code(r)
                if resp_code == 204:
                    pass
                else:
                    body = r.json()
                    debug("Cannot delete model {} {}".format(model["name"], body["message"]), _WARNING)
    else:
        debug("Cannot fetch models {}".format(body["message"]), _WARNING)
        return False


def read_looker_files(client_properties, folder_name):
    """
    :param client_properties:
    :param folder_name:
    :return:
    """
    debug("*** Function call - {}".format(sys._getframe().f_code.co_name), _INFO)

    look_files=[]
    dashboard_files=[]
    dirs = os.listdir(folder_name)
    if "input_file" in client_properties and client_properties["input_file"]:
        debug("Reading input file {}".format(client_properties["input_file"]), _INFO)

        with open(client_properties["input_file"], "r") as read_file:
            if client_properties["input_file"].endswith(_LOOK_EXT):
                look_files.append(get_json_prop(read_file))
            elif client_properties["input_file"].endswith(_DASHBOARD_EXT):
                dashboard_files.append(get_json_prop(read_file))
            else:
                debug("Invalid input file - extension does not match Look or Dashboard ", _WARNING)


    else:
        for file in dirs:
            if file.endswith(_LOOK_EXT):
                debug("Found look: {} ".format(file), _INFO)
                with open(os.path.join(folder_name, file), "r") as read_file:
                    look_files.append(get_json_prop(read_file))
            elif file.endswith(_DASHBOARD_EXT):
                debug("Found dashboard: {} ".format(file), _INFO)
                with open(os.path.join(folder_name, file), "r") as read_file:
                    dashboard_files.append(get_json_prop(read_file))

    return (look_files, dashboard_files)

def process_explore(client_prop, current_access_token, model_name, explore_name, data_dir):
    """
    Function returns
    :param client_prop:
    :param current_access_token:
    :param model_name
    :param explore_name
    :param data_dir
    :return: dictionary of models and explores
    """
    global _GLOBAL_SUMMARY
    _GLOBAL_SUMMARY.append("Validated explore {}:{}".format(explore_name, model_name))
    explore = looker_get_explore(client_prop, current_access_token, model_name, explore_name)
    if explore:
        query = looker_create_explore_query(client_prop, current_access_token, explore)
        save_json_file(data_dir, "{}_exp.json".format(explore["id"].replace(":","_")), query)
        result = looker_run_query(client_prop, current_access_token, query, "json")
        if result:
            save_json_file(data_dir, "{}_result.json".format(explore["id"].replace(":","_")), result)
    else:
        _GLOBAL_SUMMARY.append("ERROR: Error getting explore definition")
        debug("Error getting explore", _ERROR)

    return (explore, query, result)


def validate_dashboard(client_prop, current_access_token, dashboard, data_dir):
    """
    Function returns
    :param client_prop:
    :param current_access_token:
    :param dashboard
    :param data_dir
    :return: dictionary of models and explores
    """
    global _GLOBAL_SUMMARY
    dashboard_filters = dashboard["dashboard_filters"]
    for elem in dashboard["dashboard_elements"]:
        query = elem["query"]
        if not query:
            debug("Skipping element: {}".format(elem["title"]))
            continue

        if client_prop.get("server_version","5.16") == "5.18":
            # We need to get listen tags from result maker.
            if "filterables" in elem["result_maker"].keys() and elem["result_maker"]["filterables"]:
                filterables = elem["result_maker"]["filterables"][0]

            listen = {lst["dashboard_filter_name"]: lst["field"] for lst in filterables.get("listen", [])}
        else:
            if "listen" not in elem.keys():
                debug("WARNING: Listen tag not found. Make sure Server version is correct.", _WARNING)
                debug("WARNING: Most likely it is 5.18+,  but not specificed in property server_version.", _WARNING)
            else:
                listen = elem["listen"]
        filters = query["filters"]
        for key in _QUERY_KEYS_TO_REMOVE:
            del query[key]

        if not filters:
            filters = {}
        # Set dashboard default filters
        for dashboard_filter in dashboard_filters:
            if dashboard_filter["default_value"] and dashboard_filter["name"] in listen.keys():
                default_value = dashboard_filter["default_value"]
                # This is a "hack". If the field is enumeration, Looker requires ^_ as a default value or it does not accept it.
                if dashboard_filter["field"] and dashboard_filter["field"]["enumerations"]:
                    # Now we try to match the value if found
                    default_value = default_value.replace("_", "^_")
                    new_default_value = None
                    for en in dashboard_filter["field"]["enumerations"]:
                        if en["label"] == default_value:
                            new_default_value = en["value"]
                        if en["value"] == default_value:
                            new_default_value = default_value
                    default_value = new_default_value

                if default_value:
                    filters[listen[dashboard_filter["name"]]] = default_value
                else:
                    debug("Could not match default value {} with enums allowed {}".format(dashboard_filter["default_value"], dashboard_filter["field"]["enumerations"]), _WARNING)

        # process global default filters
        for dashboard_filter in listen.values():
            if client_prop.get("dashboard_default_filters", None) and dashboard_filter in client_prop["dashboard_default_filters"].keys():
                filters[dashboard_filter] = client_prop["dashboard_default_filters"][dashboard_filter]

        query["filters"] = filters
        # Navigate filters and set default
        save_json_file(data_dir,
                       "{}_{}_query.json".format(dashboard["id"].replace(":", "_"),
                                                 elem["title"].replace(" ","").replace("\\","").replace("/","")),
                       query)
        _GLOBAL_SUMMARY.append("Element: {}".format(elem["title"]))
        result = looker_run_query(client_prop, current_access_token, query, "json", elem["title"])
        if result:
            save_json_file(data_dir, "{}_{}_result.json".format(
                dashboard["id"].replace(":","_"),
                elem["title"].replace(" ","").replace("\\","").replace("/","")
            ), result)

    return True

def main():
    debug("Processing looker properties file", _INFO)
    # Disable SSL warning.
    requests.packages.urllib3.disable_warnings()
    parseArgs = argparse.ArgumentParser(description='Provide the following parameters:')
    parseArgs.add_argument('action', metavar='action', type=str,
                           help='Please provide action: exportspace, importspace. Mandatory')
    parseArgs.add_argument('-prop_file', type=argparse.FileType('r', encoding='UTF-8'),
                           help='Please provide json formatted properties file name', required=False,
                           default='looker_properties.json')

    parseArgs.add_argument('-internal_prop_file', type=argparse.FileType('r', encoding='UTF-8'),
                           help='Please provide json formatted properties file name', required=False,
                           default='internal_looker_properties.json')

    parseArgs.add_argument('-input_file',  type=str, help='Input file for a single file mode', required=False)

    args = parseArgs.parse_args()

    debug("Arguments passed: {}".format(vars(args)), _INFO)

    # Read internal properties
    debug("Reading Looker internal properties", _INFO)
    _internal_prop = get_json_prop(args.internal_prop_file)

    # Read Client properties required for deployment
    debug("Reading client properties file.", _INFO)
    _client_prop = get_json_prop(args.prop_file)

    debug("Constructing combined properties dictionary for application consumption", _INFO)
    client_prop = {**_internal_prop, **_client_prop}

    global _DEBUG_LEVEL
    _DEBUG_LEVEL = _DEBUG_CONF_LEVEL.get(client_prop.get("debug_level", "X"), _DEBUG_LEVEL)

    if "input_file" in args:
        debug("Single File Mode: {}".format( args.input_file), _INFO)
        client_prop["input_file"] = args.input_file

    debug("Setting up deployment environment folder structure", _INFO)
    # Setup Client folder and log folder under it for deployment and configuration

    os.environ['MN_LOOKER_DEPLOYMENT_BASE'] = os.path.expanduser(client_prop["looker_deployment_base"])

    if os.environ.get('MN_LOOKER_DEPLOYMENT_BASE') is None:
        debug("MN_LOOKER_DEPLOYMENT_BASE is not defined. Set it in properties file and try again. Exiting...", _ERROR)
        exit(1)
    else:
        _DEPLOYMENT_BASE = os.environ['MN_LOOKER_DEPLOYMENT_BASE']
        debug("Looker Deployment Base directory: {}".format(_DEPLOYMENT_BASE), _INFO)
        # Create Data specific deployment directory
        DATA_DIR = os.path.join(_DEPLOYMENT_BASE, client_prop["data_directory"])

        # Define logging directory
        CLIENT_DEPLOYMENT_DIR_LOG = os.path.join(_DEPLOYMENT_BASE, client_prop["log_directory"])

        # Check if Customer directory exists
        if not os.path.isdir(DATA_DIR):
            debug("Data directory {} does not exist and will be created".format(DATA_DIR), _INFO)
            os.mkdir(DATA_DIR)
        else:
            debug("Data directory {} exists, skipping creation".format(DATA_DIR), _INFO)

        # Check if log directiry exists under Customer folder
        if not os.path.isdir(CLIENT_DEPLOYMENT_DIR_LOG):
            debug("Log directory {} does not exist and will be created".format(CLIENT_DEPLOYMENT_DIR_LOG))
            os.mkdir(CLIENT_DEPLOYMENT_DIR_LOG)
        else:
            debug("Log directory {} exists".format(CLIENT_DEPLOYMENT_DIR_LOG), _INFO)

    # Enable logging
    log_file_short_name = "looker_utlities{}.log".format(get_date_timestamp())
    log_file_name = os.path.join(CLIENT_DEPLOYMENT_DIR_LOG, log_file_short_name)

    logging.basicConfig(filename=log_file_name, level=logging.INFO)
    global _LOGGER
    _LOGGER = logging.getLogger()
    debug("Looker Base directory: {}".format(_DEPLOYMENT_BASE), _INFO)
    debug("Customer Deployment directory: {}".format(DATA_DIR), _INFO)
    os.chdir(_DEPLOYMENT_BASE)

    debug("Getting Looker access token", _INFO)
    current_access_token = get_access_token(client_prop)
    space_list=looker_spaces(client_prop, current_access_token)

    global _FILTER_DEFAULT_VALUES, _GLOBAL_SUMMARY
    if "default_filters" in client_prop.keys():
        _FILTER_DEFAULT_VALUES = client_prop["default_filters"]

    # Validate provided namespaces
    if "exportspace" in args.action.lower():
        for spacename in client_prop["space_remap"].keys():
            if spacename:
                space_id = space_list[spacename]
                if not space_id:
                    debug("Cannot find specified spaces. Make sure it exists", _ERROR)
                    return

    # Validate to namespaces
    if "importspace" in args.action.lower():
        for spacename in client_prop["space_remap"].values():
            if spacename and not space_list.get(spacename,""):
                space_id = space_list[spacename]
                if not space_id:
                    debug("Cannot find specified spaces. Make sure it exists", _ERROR)
                    return

    client_prop["current_user_id"] = looker_who_am_I (client_prop, current_access_token)
    if not client_prop["current_user_id"]:
        debug("Cannot get current user info", _ERROR)
        return

    if "exportspace" in args.action.lower():
        debug("******************Running exportspace************************", _INFO)
        looks = looker_looks(client_prop, current_access_token)

        for look in looks:
            file_name = "{}__{}_{}".format(look["space"]["name"], look["title"],_LOOK_EXT).replace(" ", "_")
            with open(os.path.join(DATA_DIR, file_name), 'w') as outfile:
                json.dump(look["look_json"], outfile, indent=3, sort_keys=True)

        dashboards = looker_dashboards(client_prop, current_access_token)

        for dashboard in dashboards:
            file_name = "{}__{}_{}".format(dashboard["space"]["name"], dashboard["title"], _DASHBOARD_EXT).replace(" ", "_")
            with open(os.path.join(DATA_DIR, file_name), 'w') as outfile:
                json.dump(dashboard["dashboard_json"], outfile, indent=3, sort_keys=True)

    if "importspace" in args.action.lower():
        debug("******************Running importspace************************", _INFO)
        looks, dashboards = read_looker_files(client_prop, DATA_DIR)

        if looks:
            for look in looks:
                looker_create_look(client_prop, current_access_token, look, space_list)
        else:
            debug("Looks not found to import", _WARNING)

        if dashboards:
            # Before we create any, we need to fetch latest looks
            server_looks = looker_looks(client_prop, current_access_token, all_looks=True)

            for dashboard in dashboards:
                if not looker_create_dashboard(client_prop, current_access_token, server_looks, dashboard, space_list):
                    debug("Error during dashboard creation. Aborting process.", _ERROR)

        else:
            debug("Dashboards not found to import", _WARNING)

    if "validateexplore" in args.action.lower():
        debug("******************Running GETEXPLORES************************", _INFO)
        if "explores" in client_prop.keys() and client_prop["explores"]:
            for (mod, exp) in client_prop["explores"].items():
                process_explore(client_prop, current_access_token, mod, exp, DATA_DIR)
        else:
            models = looker_get_lookml_models(client_prop, current_access_token, client_prop["project_name"])
            for model in models:
                if "models" not in client_prop.keys() or (not client_prop["models"]) or model["name"] in client_prop["models"]:
                    for model_explore in model["explores"]:
                        process_explore(client_prop, current_access_token, model["name"],model_explore, DATA_DIR)

    if "validatedashboard" in args.action.lower():
        debug("******************Running validate Dashboard************************", _INFO)
        if client_prop.get("models", None) and not client_prop.get("dashboards", None):
            # We get models from config only is dashboards not provided explicitly.
            model_list = client_prop["models"]
        else:
            model_list = [model["name"] for model in looker_get_lookml_models(client_prop, current_access_token, client_prop["project_name"])]

        if client_prop.get("dashboards", None):
            dashboards = [{"model": {"id": k}, "id": "{}::{}".format(k,v)} for (k,v) in client_prop["dashboards"].items()]
        else:
            dashboards = looker_dashboards(client_prop, current_access_token, False)



        for meta_dashboard in dashboards:
            if meta_dashboard["model"] and meta_dashboard["model"]["id"] in model_list:
                log_msg = "Found dashboard {}".format(meta_dashboard["id"])
                _GLOBAL_SUMMARY.append("Dashboard {}".format(meta_dashboard["id"]))
                debug(log_msg, _INFO)
                dashboard = looker_get_dashboard(client_prop, current_access_token, meta_dashboard["id"])
                save_json_file(DATA_DIR, "{}_dash.json".format(dashboard["id"].replace(":","_")) ,dashboard)
                validate_dashboard(client_prop, current_access_token, dashboard, DATA_DIR)

    if "cleanupmodels" in args.action.lower():
        debug("******************Running MODEL cleanup ************************", _INFO)
        looker_cleanup_models(client_prop, current_access_token)

    debug("******************Execution Summary************************", _INFO)
    for log_line in _GLOBAL_SUMMARY:
        debug(log_line, _INFO)



# Main execution.
if __name__ == '__main__':
    main()

