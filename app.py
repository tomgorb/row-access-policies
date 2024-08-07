import os
import re
import json 
import yaml
import math
import requests
import googleapiclient.discovery
import google.auth.transport.requests
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound, BadRequest

col1, col2 = st.columns([2,11])
with col1:
    st.text('')
    st.text('')
    st.text("👋")
    st.text('')
with col2:
    st.text('')
    st.text('')
    st.text('[POC] Row Access Policies')
    st.text('')

def get_policy(crm_service, project_id, version=3):

    policy = (
        crm_service.projects()
        .getIamPolicy(
            resource=project_id,
            body={"options": {"requestedPolicyVersion": version}},
        )
        .execute()
    )

    return policy

def set_policy(crm_service, project_id, policy):

    policy = (
        crm_service.projects()
        .setIamPolicy(resource=project_id, body={"policy": policy})
        .execute()
    )

    return policy


with open("params.yaml", "r") as f:
    params = yaml.safe_load(f)

try:
    CREDS = json.loads(os.environ["GCP_SA"])
    POC_PWD = os.environ["POC_PWD"]
    PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    PROJECT_ID_VIEW = os.environ["GCP_PROJECT_ID_VIEW"]
except Exception as e:
    st.error("No %s environment variable found."%e)
    st.stop()

params.update({
    "PROJECT_ID": PROJECT_ID,
    "PROJECT_ID_VIEW": PROJECT_ID_VIEW
    })

CREATE_POLICY = """ CREATE OR REPLACE ROW ACCESS POLICY `{user}`
                    ON `{PROJECT_ID}.{TARGET_DATASET}.{TABLE_ID}_part_{part}` 
                    GRANT TO ('user:{email}') 
                    FILTER USING (identifier = '{identifier}' AND _index = FARM_FINGERPRINT('{identifier}'));"""

DROP_POLICY = """ DROP ROW ACCESS POLICY `{user}`
                  ON `{PROJECT_ID}.{TARGET_DATASET}.{TABLE_ID}_part_{part}`;"""

GRANT_DATA_VIEWER = """ GRANT `roles/bigquery.dataViewer` 
                        ON SCHEMA `{PROJECT_ID_VIEW}`.{VIEWS_DATASET} 
                        TO 'user:{email}'; """

REVOKE_DATA_VIEWER = """ REVOKE `roles/bigquery.dataViewer` 
                         ON SCHEMA `{PROJECT_ID_VIEW}`.{VIEWS_DATASET} 
                         FROM 'user:{email}'; """

def authorize_view(client, remove=False, **params):
    source_dataset = client.get_dataset(params['TARGET_DATASET'])
    view_reference = { "projectId": params['PROJECT_ID_VIEW'],
                       "datasetId": params['VIEWS_DATASET'],
                       "tableId": params['VIEW_NAME'] }
    access_entries = source_dataset.access_entries
    if remove:
        access_entries.remove(bigquery.AccessEntry(None, 'view', view_reference))
    else:
        access_entries.append(bigquery.AccessEntry(None, "view", view_reference))
    source_dataset.access_entries = access_entries
    source_dataset = client.update_dataset(source_dataset, ["access_entries"])

credentials = service_account.Credentials.from_service_account_info(CREDS,  
                                                                    scopes=['https://www.googleapis.com/auth/cloud-platform'])
bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials, location='EU')
crm_service = googleapiclient.discovery.build("cloudresourcemanager", "v1", credentials=credentials)
role = "roles/bigquery.jobUser"

st.info("Email addresses and domains must be associated with an active Google Account, Google Workspace account, or Cloud Identity account.")

email = st.text_input("email:")
regex_user = re.compile(r'^([0-9A-Za-zÀ-ÖØ-öø-ÿ]+)([-._0-9A-Za-zÀ-ÖØ-öø-ÿ]?)+@[0-9A-Za-z]+(\.[A-Z|a-z]{2,})+$')

valid_email = False
if len(email) != 0:
    if re.fullmatch(regex_user, email):
        valid_email = True
else:
    st.write("Please provide an email")

with st.expander("Grant Access"):
    if valid_email:
        identifier = st.text_input("Identifier:")
        regex_identifier = re.compile(r'([0-9])+')
        if len(identifier) != 0:
            if re.fullmatch(regex_identifier, identifier):
                if st.button('✊',key="grant"):
                    policy_name_query = "SELECT `udfs.atoz`('%s',FALSE,TRUE)"%(re.match("([^@])*",email).group(0))
                    query_job = bq_client.query(policy_name_query)
                    result = query_job.result()
                    for row in result:
                        policy_name = row[0]
                        break

                    part = math.ceil((int(identifier[::-1][0])+1)/(10/params['PARTS']))
                    st.write("Creating policy %s (part %d)"%(policy_name, part))

                    with st.spinner():
                        try:
                            query = CREATE_POLICY.format(user=policy_name, email=email, identifier=identifier, part=part, **params)
                            query_job = bq_client.query(query)
                            result = query_job.result()
                            st.success("Policy %s created!"%policy_name, icon="✅")
                        except:
                            st.error("Error creating policy %s!"%policy_name, icon="❌")

                        try:                                
                            query = GRANT_DATA_VIEWER.format(email=email, **params)
                            query_job = bq_client.query(query)
                            result = query_job.result()
                            st.success("dataViewer role granted!", icon="✅")
                        except:
                            st.error("Error granting dataViewer role!", icon="❌")
                            
                        try:
                            policy = get_policy(crm_service, PROJECT_ID_VIEW)
                            binding = None
                            for b in policy["bindings"]:
                                if b["role"] == role:
                                    binding = b
                                    break
                            if binding is not None:
                                binding["members"].append("user:%s"%email)
                            else:
                                binding = {"role": role, "members": ["user:%s"%email]}
                                policy["bindings"].append(binding)
                            set_policy(crm_service,  PROJECT_ID_VIEW, policy)
                            st.success("jobUser role granted! (IAM)", icon="✅")
                        except:
                            st.error("Error granting jobUser role! (IAM)", icon="❌")
                            
            else:
                st.write("Invalid store ID")                

with st.expander("Revoke Access"):
    if valid_email and st.button('✊',key="revoke"):
        policy_name_query = "SELECT `udfs.atoz`('%s',FALSE,TRUE)"%(re.match("([^@])*",email).group(0))
        query_job = bq_client.query(policy_name_query)
        result = query_job.result()
        for row in result:
            policy_name = row[0]
            break

        try:
            query = REVOKE_DATA_VIEWER.format(email=email, **params)
            query_job = bq_client.query(query)
            result = query_job.result()
            st.success("dataViewer role revoked!", icon="✅")
        except:
            st.error("Error revoking dataViewer role!", icon="❌")

        for i in range(1,params['PARTS']+1):
            query = DROP_POLICY.format(user=policy_name, part=i, **params)
            try:
                query_job = bq_client.query(query)
                result = query_job.result()
                st.success("Policy %s deleted (part %d)!"%(policy_name,i), icon="✅")
            except NotFound as nf:
                st.success("No policy %s found (part %d)!"%(policy_name,i), icon="✅")

        try:
            policy = get_policy(crm_service,  PROJECT_ID_VIEW)
            binding = None
            for b in policy["bindings"]:
                if b["role"] == role:
                    binding = b
                    break
            if binding is not None:
                if "members" in binding and "user:%s"%email in binding["members"]:
                    binding["members"].remove("user:%s"%email)
                    set_policy(crm_service,  PROJECT_ID_VIEW, policy)
                    st.success("jobUser role revoked ! (IAM)", icon="✅")
                else:
                    st.success("jobUser role notFound ! (IAM)", icon="✅")
            else:
                st.success("jobUser role notFound ! (IAM)", icon="✅")
        except Exception as e:
            st.exception(e)
            st.error("Error revoking jobUser role! (IAM)", icon="❌")

with st.expander("Check Access"):
    if valid_email and st.button('✊',key="check"):
        policy_name_query = "SELECT `udfs.atoz`('%s',FALSE,TRUE)"%(re.match("([^@])*",email).group(0))
        query_job = bq_client.query(policy_name_query)
        result = query_job.result()
        for row in result:
            policy_name = row[0]
            break

        policies=0
        for i in range(1,params['PARTS']+1):
            url = 'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT_ID}/datasets/{TARGET_DATASET}/tables/{tableId}/rowAccessPolicies'
            tableId = "{TABLE_ID}_part_{part}".format(part=i, **params)
            
            try:
                auth_req = google.auth.transport.requests.Request()
                credentials.refresh(auth_req)
                response = requests.get(url.format(tableId=tableId, **params), headers={'Authorization': 'Bearer %s' %credentials.token}).json()
                for policy in response['rowAccessPolicies']:
                    if policy['rowAccessPolicyReference']['policyId'] == policy_name:
                        policies+=1
                        st.info("Policy {policy} found with filter {filter}".format(policy=policy_name, filter=policy['filterPredicate']), icon='ℹ️')
            except Exception as e:
                st.error(e)  
        if policies == 0:
            st.info("No policy {policy} found!".format(policy=policy_name), icon="ℹ️")

with st.expander("Check IAM Policies"):
    if st.button('✊',key="checkIAM"):
        policy_name_query = "SELECT `udfs.atoz`('%s',FALSE,TRUE)"%(re.match("([^@])*",email).group(0))
        query_job = bq_client.query(policy_name_query)
        result = query_job.result()
        for row in result:
            policy_name = row[0]
            break

        try:
            policy = get_policy(crm_service,  PROJECT_ID_VIEW)
            binding = None
            for b in policy["bindings"]:
                if b["role"] == role:
                    binding = b
                    break
            if binding is not None:
                st.write(binding)
        except Exception as e:
            st.exception(e)
            st.error("Error checking jobUser role! (IAM)", icon="❌")
                
with st.expander("View Authorization"):
    with st.form(key='my_form', clear_on_submit=True):
        option = st.selectbox('Option',['', 'Authorize', 'Deauthorize'], label_visibility="collapsed")    
        pwd = st.text_input(label='Your passphrase', type="password", label_visibility="collapsed")
        submit_button = st.form_submit_button(label='Submit')
        if submit_button:
            if pwd == POC_PWD:
                if option == "Authorize":
                    try:
                        authorize_view(bq_client, **params)
                        st.success('Authorized.')
                    except BadRequest as br:
                        st.error("Duplicate authorized views")
                    except Exception as e:
                        st.error(e.message)
                if option == "Deauthorize":
                    try:
                        authorize_view(bq_client, remove=True, **params)
                        st.success('Deauthorized.')
                    except:
                        st.error("Nothing to do")
            else:
                st.error("Wrong passphrase")
