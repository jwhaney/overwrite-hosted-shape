'''
This script automates the process of updating an arcgis online hosted shapefile/feature layer.
The data is sent by email every week. This script is called after the email attachment (.zip
shapefile) is downloaded to a specified directory. The .zip is uploaded to AGO and overwrites
an existing hosted shapefile with the same name.

Automated using Windows Task Scheduler to run weekly.

Python 2.7

Credit to GIS StackExchange user KHibma's answer here:
https://gis.stackexchange.com/questions/208763/overwrite-feature-class-in-arcgis-online-using-python

Scroll to line 366 to begin entering your information
'''

import os, sys, time
import urllib, urllib2
import json, mimetypes
import gzip
from io import BytesIO
import string
import random

class AGOLHandler(object):

    def __init__(self, username, password, serviceName, folderName):

        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': ('updatehostedfeatureservice')
        }
        self.username = username
        self.password = password
        self.base_url = "https://www.arcgis.com/sharing/rest"
        self.serviceName = serviceName
        self.token = self.getToken(username, password)
        self.itemID = self.findItem("Feature Service")
        self.SDitemID = self.findItem("Shapefile")
        self.folderName = folderName
        self.folderID = self.findFolder()

    def getToken(self, username, password, exp=60):

        referer = "http://www.arcgis.com/"
        query_dict = {'username': username,
                      'password': password,
                      'expiration': str(exp),
                      'client': 'referer',
                      'referer': referer,
                      'f': 'json'}

        token_url = '{}/generateToken'.format(self.base_url)

        token_response = self.url_request(token_url, query_dict, 'POST')

        if "token" not in token_response:
            print(token_response['error'])
            sys.exit()
        else:
            return token_response['token']

    def findItem(self, findType):
        """ Find the itemID of whats being updated
        """

        searchURL = self.base_url + "/search"

        query_dict = {'f': 'json',
                      'token': self.token,
                      'q': "title:\"" + self.serviceName + "\"AND owner:\"" +
                      self.username + "\" AND type:\"" + findType + "\""}

        jsonResponse = self.url_request(searchURL, query_dict, 'POST')

        if jsonResponse['total'] == 0:
            print("\nCould not find a service to update. Check the service name in the settings.ini")
            sys.exit()
        else:
            resultList = jsonResponse['results']
            for it in resultList:
                if it["title"] == self.serviceName:
                    print("found {} : {}").format(findType, it["id"])
                    return it["id"]

    def findFolder(self, folderName=None):
        """ Find the ID of the folder containing the service
        """

        if self.folderName == "None" or folderName is None:
            return ""

        findURL = "{}/content/users/{}".format(self.base_url, self.username)

        query_dict = {'f': 'json',
                      'num': 1,
                      'token': self.token}

        jsonResponse = self.url_request(findURL, query_dict, 'POST')

        for folder in jsonResponse['folders']:
            if folder['title'] == self.folderName:
                return folder['id']

        print("\nCould not find the specified folder name provided in the settings.ini")
        print("-- If your content is in the root folder, change the folder name to 'None'")
        sys.exit()

    def upload(self, fileName, tags, description):
        """
         Overwrite the SD on AGOL with the new SD.
         This method uses 3rd party module: requests
        """

        updateURL = '{}/content/users/{}/{}/items/{}/update'.format(self.base_url, self.username,
                                                                    self.folderID, self.SDitemID)

        query_dict = {"filename": fileName,
                      "type": "Shapefile",
                      "title": self.serviceName,
                      "tags": tags,
                      "description": description,
                      "f": "json",
                      'multipart': 'true',
                      "token": self.token}

        details = {'filename': fileName}
        add_item_res = self.url_request(updateURL, query_dict, "POST", "", details)

        itemPartJSON = self._add_part(fileName, add_item_res['id'], "Shapefile")

        if "success" in itemPartJSON:
            itemPartID = itemPartJSON['id']

            commit_response = self.commit(itemPartID)

            # valid states: partial | processing | failed | completed
            status = 'processing'
            while status == 'processing' or status == 'partial':
                status = self.item_status(itemPartID)['status']
                time.sleep(1.5)

            print("updated SD:   {}".format(itemPartID))
            return True

        else:
            print("\n.sd file not uploaded. Check the errors and try again.\n")
            print(itemPartJSON)
            sys.exit()

    def _add_part(self, file_to_upload, item_id, upload_type=None):
        """ Add the item to the portal in chunks.
        """

        def read_in_chunks(file_object, chunk_size=10000000):
            """Generate file chunks of 10MB"""
            while True:
                data = file_object.read(chunk_size)
                if not data:
                    break
                yield data

        url = '{}/content/users/{}/items/{}/addPart'.format(self.base_url, self.username, item_id)

        with open(file_to_upload, 'rb') as f:
            for part_num, piece in enumerate(read_in_chunks(f), start=1):
                title = os.path.basename(file_to_upload)
                files = {"file": {"filename": file_to_upload, "content": piece}}
                params = {
                    'f': "json",
                    'token': self.token,
                    'partNum': part_num,
                    'title': title,
                    'itemType': 'file',
                    'type': upload_type
                }

                request_data, request_headers = self.multipart_request(params, files)
                resp = self.url_request(url, request_data, "MULTIPART", request_headers)

        return resp

    def item_status(self, item_id, jobId=None):
        """ Gets the status of an item.
        Returns:
            The item's status. (partial | processing | failed | completed)
        """

        url = '{}/content/users/{}/items/{}/status'.format(self.base_url, self.username, item_id)
        parameters = {'token': self.token,
                      'f': 'json'}

        if jobId:
            parameters['jobId'] = jobId

        return self.url_request(url, parameters)

    def commit(self, item_id):
        """ Commits an item that was uploaded as multipart
        """

        url = '{}/content/users/{}/items/{}/commit'.format(self.base_url, self.username, item_id)
        parameters = {'token': self.token,
                      'f': 'json'}

        return self.url_request(url, parameters)

    def publish(self, summary, maxRecords):
        """ Publish the existing SD on AGOL (it will be turned into a Feature Service)
        """

        publishURL = '{}/content/users/{}/publish'.format(self.base_url, self.username)

        query_dict = {'itemID': self.SDitemID,
                      'filetype': 'shapefile',
                      'overwrite': 'true',
                      'f': 'json',
                      'token': self.token,
                      'publishParameters' : {"name":self.serviceName,
                                             'maxRecordCount':maxRecords,
                                             'description':summary}
                      }

        jsonResponse = self.url_request(publishURL, query_dict, 'POST')
        try:
            if 'jobId' in jsonResponse['services'][0]:
                jobID = jsonResponse['services'][0]['jobId']

                # valid states: partial | processing | failed | completed
                status = 'processing'
                print("Checking the status of publish..")
                while status == 'processing' or status == 'partial':
                    status = self.item_status(self.SDitemID, jobID)['status']
                    print("  {}".format(status))
                    time.sleep(2)

                if status == 'completed':
                    print("item finished published")
                    return jsonResponse['services'][0]['serviceItemId']
                if status == 'failed':
                    raise("Status of publishing returned FAILED.")

        except Exception as e:
            print("Problem trying to check publish status. Might be further errors.")
            print("Returned error Python:\n   {}".format(e))
            print("Message from publish call:\n  {}".format(jsonResponse))
            print(" -- quit --")
            sys.exit()


    def enableSharing(self, newItemID, everyone, orgs, groups):
        """ Share an item with everyone, the organization and/or groups
        """

        shareURL = '{}/content/users/{}/{}/items/{}/share'.format(self.base_url, self.username,
                                                                  self.folderID, newItemID)

        if groups is None:
            groups = ''

        query_dict = {'f': 'json',
                      'everyone': everyone,
                      'org': orgs,
                      'groups': groups,
                      'token': self.token}

        jsonResponse = self.url_request(shareURL, query_dict, 'POST')

        print("successfully shared...{}...".format(jsonResponse['itemId']))

    def url_request(self, in_url, request_parameters, request_type='GET',
                    additional_headers=None, files=None, repeat=0):

        if request_type == 'GET':
            req = urllib2.Request('?'.join((in_url, urllib.urlencode(request_parameters))))
        elif request_type == 'MULTIPART':
            req = urllib2.Request(in_url, request_parameters)
        else:
            req = urllib2.Request(
                in_url, urllib.urlencode(request_parameters), self.headers)

        if additional_headers:
            for key, value in list(additional_headers.items()):
                req.add_header(key, value)
        req.add_header('Accept-encoding', 'gzip')

        response = urllib2.urlopen(req)

        if response.info().get('Content-Encoding') == 'gzip':
            buf = BytesIO(response.read())
            with gzip.GzipFile(fileobj=buf) as gzip_file:
                response_bytes = gzip_file.read()
        else:
            response_bytes = response.read()

        response_text = response_bytes.decode('UTF-8')
        response_json = json.loads(response_text)

        if not response_json or "error" in response_json:
            rerun = False
            if repeat > 0:
                repeat -= 1
                rerun = True

            if rerun:
                time.sleep(2)
                response_json = self.url_request(
                    in_url, request_parameters, request_type,
                    additional_headers, files, repeat)

        return response_json

    def multipart_request(self, params, files):
        """ Uploads files as multipart/form-data. files is a dict and must
            contain the required keys "filename" and "content". The "mimetype"
            value is optional and if not specified will use mimetypes.guess_type
            to determine the type or use type application/octet-stream. params
            is a dict containing the parameters to be passed in the HTTP
            POST request.

            content = open(file_path, "rb").read()
            files = {"file": {"filename": "some_file.sd", "content": content}}
            params = {"f": "json", "token": token, "type": item_type,
                      "title": title, "tags": tags, "description": description}
            data, headers = multipart_request(params, files)
            """
        # Get mix of letters and digits to form boundary.
        letters_digits = "".join(string.digits + string.ascii_letters)
        boundary = "----WebKitFormBoundary{}".format("".join(random.choice(letters_digits) for i in range(16)))
        file_lines = []
        # Parse the params and files dicts to build the multipart request.
        for name, value in params.iteritems():
            file_lines.extend(("--{}".format(boundary),
                               'Content-Disposition: form-data; name="{}"'.format(name),
                               "", str(value)))
        for name, value in files.items():
            if "filename" in value:
                filename = value.get("filename")
            else:
                raise Exception("The filename key is required.")
            if "mimetype" in value:
                mimetype = value.get("mimetype")
            else:
                mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            if "content" in value:
                file_lines.extend(("--{}".format(boundary),
                                   'Content-Disposition: form-data; name="{}"; filename="{}"'.format(name, filename),
                                   "Content-Type: {}".format(mimetype), "",
                                   (value.get("content"))))
            else:
                raise Exception("The content key is required.")
        # Create the end of the form boundary.
        file_lines.extend(("--{}--".format(boundary), ""))

        request_data = "\r\n".join(file_lines)
        request_headers = {"Content-Type": "multipart/form-data; boundary={}".format(boundary),
                           "Content-Length": str(len(request_data))}
        return request_data, request_headers


if __name__ == "__main__":

    print("Starting Feature Service publish process from zip (shp files)")

    # AGOL Credentials - enter your username and password
    inputUsername = 'Enter user name'
    inputPswd = 'Enter P@$$W0RD'

    # FS values - enter location of .zip file, service name on AGO, AGO folder name, tags, & summary
    ZIPFILE = "D:\\Enter_Your\\Directory\\Here\\Your_Zip_File.zip"
    serviceName = "Your_Layer_Service_Name"
    folderName = "Enter a folder name here on AGO if your data is not in root"
    tags = "overwrite, hosted, shapefile"
    summary = "This is a hosted shapefile summary"
    maxRecords = 1000

    # Share FS to: everyone, org, groups - enter sharing details, organization, groups, etc.
    shared = True
    everyone = 'false'
    orgs = 'true'
    groups = None # Groups are by ID. Multiple groups comma separated

    # initialize AGOLHandler class
    agol = AGOLHandler(inputUsername, inputPswd, serviceName, folderName)

    # overwrite the existing .SD on arcgis.com
    if agol.upload(ZIPFILE, tags, summary):

        # publish the sd which was just uploaded
        fsID = agol.publish(summary, maxRecords)

        # share the item
        if shared:
            agol.enableSharing(fsID, everyone, orgs, groups)

        print("\nfinished.")
