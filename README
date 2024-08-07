# (Proof of Concept) Row Access Policies 

> Implementing row-level security in BigQuery to restrict data visibility for individual users.

## CODE

### Environments Variables

```shell
source env.sh
```
It will cause your variables to be set in the current shell otherwise bash will open a non-interactive shell.

Content of *env.sh*
- ```export GCP_PROJECT_ID='myProject'```
- ```export GCP_PROJECT_ID_VIEW='myProjectForViews'```
- ```export GCP_SA=$(cat myServiceAccount.json)```
- ```export GCP_USER='firstname.lastname@gmail.com'```
- ```export POC_PWD='myPassword'```

#### **GCP_SA**

GCP Service Account Role(s):
- **GCP_PROJECT_ID**
  - BigQuery Admin
- **GCP_PROJECT_ID_VIEW**
  - BigQuery Data Editor 
  - Security Admin

#### **GCP_USER** 

> Email addresses and domains must be associated with an active **Google Account**, **Google Workspace account**, or **Cloud Identity account**.


### DATA

In the example data, there is a field **identifier** (a numerical string) which will be used for partitioning.

***main.py*** will load these data and prepare it:

  - copy data with an integer-based partitioning (max 10,000 partitions) on **index** using *farm fingerprinting* on **identifier** ;
  - split data into **n** parts (*params.yaml*) to somehow bypass the 100-row access policy limit per table ;
  - add a default row access policy on 0=1 so that by default no one can see the data.


### Web App 

***app.py*** is a Streamlit application to:
- grant, revoke and check accesses for a specific user (email) ;

![Grant Access](screenshots/streamlit%20-%20Grant%20Access.png)

- check IAM policies ;

![IAM Policies](screenshots/streamlit%20-%20IAM%20Policies.png)

- (de)authorize view.

![View Authorization](screenshots/streamlit%20-%20View%20Authorization.png)

The code is using a BigQuery *user defined function* **atoz** available [here](https://github.com/tomgorb/gcp-terraform-examples/blob/main/bigquery_udf/sql/atoz.sql).

### Dashboard

Create a dashboard in **Looker Studio** using the view *insights* as source.

**DO NOT FORGET** to change Data Credentials to ```Viewer```. Otherwise your (```Owner```) credentials will be used (potential data leak). 

Nevertheless if **you** $\Leftrightarrow$ **GCP_USER**, in principle, you should not see any data since there is a policy on 0=1 (kind of built-in security).

You should share the report as follow
```
Unlisted
Anyone on the internet with the link can view          Viewer 
```

- After granting access, you should see the effects almost immediately.
- After revoking access, you should wait a couple of minutes or wait for data refreshening.


![Authorized](screenshots/Looker%20Studio%20-%20view%20authorized%20AND%20access.png)

![Unauthorized](screenshots/Looker%20Studio%20-%20view%20unauthorized%20OR%20no%20access.png)
